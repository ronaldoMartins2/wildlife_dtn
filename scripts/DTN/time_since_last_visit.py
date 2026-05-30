import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from geopy.distance import geodesic

"""
Calcula métricas de uso da vizinhança dos centróides por animais,
alinhadas ao conceito de "time since last visit" (RD) e
"residence time per visit" (ID) de Benhamou & Riotte-Lambert (2012),
adaptado para círculos centrados em centroides de agrupamento.

Agora o script:
- percorre automaticamente combinações de (método, k),
- assumindo que os arquivos seguem o padrão:

    centroids_{k}_{method}_rawdata.csv
    points_{method}_mapping_{k}_centroids_rawdata.csv

Exemplo de uso:

    python scripts\DTN\\time_since_last_visit.py --base_dir scripts\Results\jaguar_mamiraua --methods birch kmeans som --ks 8 16 32 --raio_m 250 --tc_min 120
"""

# ----------------------------------------------------------------------
# Funções auxiliares para cruzamento do raio (entrada/saída) e visitas
# ----------------------------------------------------------------------


def _interp_cross_time(t0, d0, t1, d1, R):
    """
    Interpola o instante em que a distância cruza o raio R (entrada ou saída).
    Assume que a distância varia linearmente no tempo entre t0 e t1.

    Retorna:
        timestamp interpolado ou None se não houver cruzamento.
    """
    # Se não há mudança de sinal em (d - R), não há cruzamento
    if (d0 - R) * (d1 - R) >= 0:
        return None
    # fração do intervalo em que ocorre o cruzamento
    frac = (d0 - R) / (d0 - d1)
    return t0 + (t1 - t0) * frac


def _extrair_visitas(grp: pd.DataFrame, R: float, tc_min: float):
    """
    A partir de um grupo (id_animal, id_centroid) com colunas:
        - timestamp
        - dist_m
    extrai visitas ao círculo de raio R.

    Uma "visita" é um período contínuo em que o animal está dentro do raio R,
    permitindo pequenas saídas com duração ≤ TC (em minutos), que não quebram
    a visita (threshold TC inspirado em Benhamou & Riotte-Lambert, 2012).

    Retorna:
        lista de dicts:
            {
                "entry":  t_entrada (Timestamp),
                "exit":   t_saida   (Timestamp),
                "res":    residence_time (Timedelta),
            }
    """
    if grp.shape[0] < 2:
        return []

    g = grp.sort_values("timestamp").reset_index(drop=True)
    Rm = R
    tc = pd.Timedelta(minutes=tc_min)

    times = g["timestamp"].to_list()
    dists = g["dist_m"].to_list()

    visits_raw = []

    # Estado inicial: dentro ou fora do círculo
    state_in = dists[0] <= Rm
    current_entry = times[0] if state_in else None
    current_res = pd.Timedelta(0)

    # Percorre segmentos consecutivos da trajetória
    for i in range(len(g) - 1):
        t0, t1 = times[i], times[i + 1]
        d0, d1 = dists[i], dists[i + 1]

        if state_in:
            # Estado atual: dentro do círculo
            if d0 <= Rm and d1 <= Rm:
                # Todo o segmento permanece dentro
                current_res += (t1 - t0)
            elif d0 <= Rm and d1 > Rm:
                # Saída do círculo em algum ponto entre t0 e t1
                t_exit = _interp_cross_time(t0, d0, t1, d1, Rm) or t1
                current_res += (t_exit - t0)
                visits_raw.append(
                    {"entry": current_entry, "exit": t_exit, "res": current_res}
                )
                # Atualiza estado para fora
                state_in = False
                current_entry = None
                current_res = pd.Timedelta(0)
            else:
                # Casos degenerados (por segurança): força atualização de estado
                state_in = d1 <= Rm
                if state_in:
                    current_entry = t0
        else:
            # Estado atual: fora do círculo
            if d0 > Rm and d1 > Rm:
                # Permanece fora
                continue
            elif d0 > Rm and d1 <= Rm:
                # Entrada no círculo em algum ponto entre t0 e t1
                t_entry = _interp_cross_time(t0, d0, t1, d1, Rm) or t1
                state_in = True
                current_entry = t_entry
                # Se o ponto final está dentro, conta o trecho até t1
                current_res = (t1 - t_entry) if d1 <= Rm else pd.Timedelta(0)
            else:
                # Casos degenerados
                state_in = d1 <= Rm
                if state_in:
                    current_entry = t0

    # Se terminou "dentro", fecha a última visita no último timestamp
    if state_in and current_entry is not None:
        last_t = times[-1]
        # current_res já acumulou os segmentos anteriores "dentro"
        visits_raw.append({"entry": current_entry, "exit": last_t, "res": current_res})

    if not visits_raw:
        return []

    # ---- Aplicar TC: funde visitas separadas por gaps menores ou iguais a TC ----
    visits_merged = []
    current = visits_raw[0]

    for v in visits_raw[1:]:
        gap = v["entry"] - current["exit"]
        if gap <= tc:
            # Considerar como extensão da mesma visita
            current["exit"] = v["exit"]
            current["res"] += v["res"]
        else:
            visits_merged.append(current)
            current = v

    visits_merged.append(current)
    return visits_merged


def _parse_timestamp_column(df: pd.DataFrame, col: str, formato_ts: str) -> pd.Series:
    """
    Converte a coluna de timestamp aceitando:
    - um único formato strftime (ex: "%m/%d/%y %H:%M")
    - vários formatos separados por "|" (ex: "%m/%d/%y %H:%M|%Y-%m-%d %H:%M:%S.%f")
    - o valor especial "auto", que deixa o pandas inferir o formato.

    Retorna uma Series datetime64[ns].
    """
    serie = df[col]

    # Modo automático: deixa o pandas inferir
    if formato_ts.lower() == "auto":
        return pd.to_datetime(serie, errors="raise", infer_datetime_format=True)

    # Lista de formatos (1 ou vários separados por "|")
    formatos = [f.strip() for f in formato_ts.split("|") if f.strip()]

    ts_final = pd.Series(index=serie.index, dtype="datetime64[ns]")
    mask_restante = pd.Series(True, index=serie.index)

    for fmt in formatos:
        # Tenta converter apenas nas linhas ainda não convertidas
        convertidos = pd.to_datetime(
            serie[mask_restante],
            format=fmt,
            errors="coerce",
        )
        # Atribui os que deram certo
        ts_final.loc[mask_restante] = convertidos
        # Atualiza quais ainda ficaram NaT
        mask_restante = mask_restante & convertidos.isna()

        # Se todo mundo já foi convertido, pode parar
        if not mask_restante.any():
            break

    # Se ainda restou algo sem converter, tenta inferir automaticamente
    if mask_restante.any():
        ts_final.loc[mask_restante] = pd.to_datetime(
            serie[mask_restante],
            errors="raise",
            infer_datetime_format=True,
        )

    return ts_final


# ----------------------------------------------------------------------
# Função principal de cálculo (RD + ID + tempo de retorno) por (animal, centróide)
# ----------------------------------------------------------------------


def calcular_tempo_retorno_vizinhanca(
    df_traj: pd.DataFrame,
    df_cent: pd.DataFrame,
    raio_vizinhanca_m: float,
    formato_ts: str,
    tc_min: float,
) -> pd.DataFrame:
    """
    Calcula métricas de uso da vizinhança dos centroides, por (id_animal, id_centroid):

    - n_visitas        (RD: number of visits)
    - mean_res_*       (ID: mean residence time per visit)
    - median_res_*
    - mean_ret_*       (tempo médio de retorno entre visitas)
    - median_ret_*

    usando um círculo de raio 'raio_vizinhanca_m' ao redor de cada centróide
    e threshold TC (tc_min, em minutos) para fundir visitas separadas por
    curtas saídas da área.
    """

    # # Converte timestamp
    # df_traj["timestamp"] = pd.to_datetime(df_traj["timestamp"], format=formato_ts)
    # Converte timestamp (aceitando múltiplos formatos)
    df_traj["timestamp"] = _parse_timestamp_column(df_traj, "timestamp", formato_ts)


    # Cross join: cada ponto com TODOS os centroides
    df_traj["_key"] = 1
    df_cent["_key"] = 1
    df = df_traj.merge(df_cent, on="_key").drop(columns="_key")

    # Calcula distância geodésica ponto–centróide em metros
    df["dist_m"] = df.apply(
        lambda r: geodesic(
            (r["latitude_animal"], r["longitude_animal"]),
            (r["lat_centroid"], r["lon_centroid"]),
        ).meters,
        axis=1,
    )

    resultados = []

    # Processa por (animal, centróide)
    for (animal, cid), grp in df.groupby(["id_animal", "id_centroid"]):
        visitas = _extrair_visitas(
            grp[["timestamp", "dist_m"]], raio_vizinhanca_m, tc_min
        )

        if not visitas:
            continue

        n_vis = len(visitas)
        res_secs = [v["res"].total_seconds() for v in visitas]

        mean_res_s = float(np.mean(res_secs))
        median_res_s = float(np.median(res_secs))

        # Gaps entre visitas (tempo de retorno)
        if n_vis >= 2:
            gaps = [
                (visitas[i]["entry"] - visitas[i - 1]["exit"]).total_seconds()
                for i in range(1, n_vis)
            ]
            mean_ret_s = float(np.mean(gaps))
            median_ret_s = float(np.median(gaps))
        else:
            mean_ret_s = float("nan")
            median_ret_s = float("nan")

        resultados.append(
            {
                "id_animal": animal,
                "id_centroid": cid,
                "n_visitas": n_vis,
                # Residence time (ID)
                "mean_res_s": mean_res_s,
                "mean_res_h": mean_res_s / 3600.0,
                "mean_res_d": mean_res_s / 86400.0,
                "median_res_s": median_res_s,
                "median_res_h": median_res_s / 3600.0,
                "median_res_d": median_res_s / 86400.0,
                # Tempo de retorno (entre visitas)
                "mean_ret_s": mean_ret_s,
                "mean_ret_h": mean_ret_s / 3600.0
                if np.isfinite(mean_ret_s)
                else np.nan,
                "mean_ret_d": mean_ret_s / 86400.0
                if np.isfinite(mean_ret_s)
                else np.nan,
                "median_ret_s": median_ret_s,
                "median_ret_h": median_ret_s / 3600.0
                if np.isfinite(median_ret_s)
                else np.nan,
                "median_ret_d": median_ret_s / 86400.0
                if np.isfinite(median_ret_s)
                else np.nan,
            }
        )

    if not resultados:
        return pd.DataFrame()

    return pd.DataFrame(resultados)


# ----------------------------------------------------------------------
# main() com argparse – AGORA LOOPANDO SOBRE MÉTODO E K
# ----------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Calcula RD (n_visitas) + ID (residence time por visita) + "
            "tempo de retorno entre visitas às vizinhanças dos centroides, "
            "para várias combinações de (método, k), assumindo padrões de nome "
            "dos arquivos: centroids_{k}_{method}_rawdata.csv e "
            "points_{method}_mapping_{k}_centroids_rawdata.csv."
        )
    )

    parser.add_argument(
        "--base_dir",
        required=True,
        help="Diretório base onde estão os CSVs de centroides e trajetos.",
    )
    parser.add_argument(
        "--methods",
        nargs="+",
        default=["birch"],
        help="Lista de métodos (strings usadas nos nomes dos arquivos, ex.: birch kmeans som).",
    )
    parser.add_argument(
        "--ks",
        nargs="+",
        type=int,
        default=[8, 16, 32],
        help="Lista de números de centróides (ex.: 8 16 32).",
    )
    parser.add_argument(
        "--raio_m",
        type=float,
        default=250.0,
        help="Raio de vizinhança em metros (default: 250).",
    )
    parser.add_argument(
        "--tc_min",
        type=float,
        default=120.0,
        help="Threshold TC em minutos para fundir visitas (default: 120).",
    )
    # parser.add_argument(
    #     "--formato_ts",
    #     default="%m/%d/%y %H:%M",
    #     help=(
    #         "Formato do timestamp no CSV de trajetos "
    #         "(default: %%m/%%d/%%y %%H:%%M)."
    #     ),
    # )
    parser.add_argument(
    "--formato_ts",
    default="%m/%d/%y %H:%M|%Y-%m-%d %H:%M:%S.%f",
    help=(
        "Formato do timestamp no CSV de trajetos. "
        "Pode ser um único formato strftime (ex.: '%%m/%%d/%%y %%H:%%M') "
        "ou vários formatos separados por '|', ex.: "
        "'%%m/%%d/%%y %%H:%%M|%%Y-%%m-%%d %%H:%%M:%%S.%%f'. "
        "Use 'auto' para deixar o pandas inferir."
        ),
    )


    args = parser.parse_args()

    base_dir = Path(args.base_dir)
    if not base_dir.is_dir():
        raise NotADirectoryError(f"Diretório base não existe: {base_dir}")

    for method in args.methods:
        for k in args.ks:
            # Monta nomes dos arquivos de entrada conforme o padrão informado
            traj_path = base_dir / f"points_{method}_mapping_{k}_centroids_bilstm.csv"
            cent_path = base_dir / f"centroids_{k}_{method}_bilstm.csv"

            print(f"\n=== Processando método={method}, k={k} ===")
            print(f"Trajetos : {traj_path}")
            print(f"Centroides: {cent_path}")

            if not traj_path.is_file():
                print(f"⚠ Arquivo de trajetos não encontrado, pulando: {traj_path}")
                continue
            if not cent_path.is_file():
                print(f"⚠ Arquivo de centroides não encontrado, pulando: {cent_path}")
                continue

            # Lê CSV de trajetos
            df_traj = pd.read_csv(traj_path)

            # Garante que as colunas mínimas existam
            col_min = {"id_animal", "timestamp", "latitude_animal", "longitude_animal"}
            if not col_min.issubset(df_traj.columns):
                print(
                    f"⚠ Arquivo {traj_path} não tem as colunas mínimas {col_min}. "
                    f"Colunas encontradas: {set(df_traj.columns)} – pulando."
                )
                continue

            # Ignora qualquer id_centroid que venha no CSV de trajetos
            if "id_centroid" in df_traj.columns:
                df_traj = df_traj.drop(columns=["id_centroid"])

            # Lê centroides (sem cabeçalho)
            df_cent = pd.read_csv(
                cent_path,
                header=None,
                names=["id_centroid", "lon_centroid", "lat_centroid"],
            )

            # Calcula métricas
            resultado = calcular_tempo_retorno_vizinhanca(
                df_traj=df_traj,
                df_cent=df_cent,
                raio_vizinhanca_m=args.raio_m,
                formato_ts=args.formato_ts,
                tc_min=args.tc_min,
            )

            if resultado.empty:
                print("⚠ Nenhum resultado para salvar (sem visitas detectadas).")
                continue

            # Gera nome automático de saída
            saida_path = traj_path.with_name(
                traj_path.stem
                + f"_retorno_{int(args.raio_m)}m_tc{int(args.tc_min)}min_vizinhanca.csv"
            )
            saida_path.parent.mkdir(parents=True, exist_ok=True)

            resultado.to_csv(saida_path, index=False)

            print(f"✔ Arquivo gerado: {saida_path}")
            print(resultado.head())


if __name__ == "__main__":
    main()
