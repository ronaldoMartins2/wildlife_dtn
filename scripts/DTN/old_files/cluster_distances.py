#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Calcula distância média, mínima e máxima entre TODOS os animais e cada centróide.

Uso (exemplo):

python cluster_distances.py ^
  --animals C:\...\map_jaguar_mamiraua_all_animals.csv ^
  --centroids-dir C:\...\jaguar_mamiraua ^
  --output-dir C:\...\jaguar_mamiraua
"""

import argparse
import pathlib
import pandas as pd
from geopy.distance import geodesic


# ---------------------------------------------------------
# Identificação de colunas lat/lon
# ---------------------------------------------------------
def find_lat_lon_columns(df):
    """
    Tenta identificar automaticamente colunas de latitude e longitude.

    CASO 1: sem cabeçalho (colunas numéricas 0,1,2,3...):
        - se tiver 4 colunas -> 0=id, 1=ts, 2=lon, 3=lat   (animais)
        - se tiver 3 colunas -> 0=id, 1=lon, 2=lat         (centróides)

    CASO 2: com cabeçalho textual -> tenta por nomes/aliases.
    """
    cols = list(df.columns)

    # CASO 1: sem cabeçalho (colunas são inteiros)
    if all(isinstance(c, int) for c in cols):
        if len(cols) >= 4:
            # animais: id, ts, lon, lat
            lat_col = 3
            lon_col = 2
            print(f"[INFO] Sem cabeçalho (4+ colunas). Usando índices: lat={lat_col}, lon={lon_col}")
            return lat_col, lon_col
        elif len(cols) == 3:
            # centróides: id, lon, lat
            lat_col = 2
            lon_col = 1
            print(f"[INFO] Sem cabeçalho (3 colunas). Usando índices: lat={lat_col}, lon={lon_col}")
            return lat_col, lon_col
        else:
            raise ValueError(
                f"Não sei mapear lat/lon para {len(cols)} colunas sem cabeçalho: {cols}"
            )

    # CASO 2: com cabeçalho textual
    lat_alias = ["lat", "latitude", "y", "y_deg"]
    lon_alias = ["lon", "longitude", "long", "x", "x_deg"]

    lower_map = {str(c).lower(): c for c in cols}

    # 1) nomes exatos
    lat = next((lower_map[a] for a in lat_alias if a in lower_map), None)
    lon = next((lower_map[a] for a in lon_alias if a in lower_map), None)

    # 2) fallback: substrings
    if lat is None:
        lat = next((c for c in cols if "lat" in str(c).lower()), None)
    if lon is None:
        lon = next(
            (c for c in cols if ("lon" in str(c).lower()) or ("lng" in str(c).lower())),
            None,
        )

    if lat is None or lon is None:
        raise ValueError(
            f"Não foi possível identificar colunas de latitude/longitude. "
            f"Colunas disponíveis: {cols}"
        )

    print(f"[INFO] Usando colunas lat/lon: {lat} / {lon}")
    return lat, lon

# ---------------------------------------------------------
# Cálculo das distâncias
# ---------------------------------------------------------
def calcular_distancias_por_centroides(df_animais, df_centroids, metodo, saida_path):

    lat_a, lon_a = find_lat_lon_columns(df_animais)
    lat_c, lon_c = find_lat_lon_columns(df_centroids)

    resultados = []

    def coord_valida(lat, lon):
        return (-90.0 <= lat <= 90.0) and (-180.0 <= lon <= 180.0)

    for _, rowc in df_centroids.iterrows():

        # ID REAL DO CENTROIDE (primeira coluna do CSV)
        centroid_id = int(rowc[0])

        c_lat = float(rowc[lat_c])
        c_lon = float(rowc[lon_c])

        if not coord_valida(c_lat, c_lon):
            print(f"[WARN] Centróide inválido (ignorado): ID={centroid_id}, lat={c_lat}, lon={c_lon}")
            continue

        distancias = []

        for _, rowa in df_animais.iterrows():
            a_lat = float(rowa[lat_a])
            a_lon = float(rowa[lon_a])

            if not coord_valida(a_lat, a_lon):
                continue

            try:
                d = geodesic((a_lat, a_lon), (c_lat, c_lon)).meters
                distancias.append(d)
            except:
                continue

        if not distancias:
            print(f"[WARN] Nenhuma distância válida para centróide ID={centroid_id}")
            continue

        resultados.append(
            {
                "centroid_id": centroid_id,  # <-- agora o ID correto
                "mean_m": sum(distancias) / len(distancias),
                "min_m": min(distancias),
                "max_m": max(distancias),
            }
        )

    df_out = pd.DataFrame(resultados)
    df_out.to_csv(saida_path, index=False)
    print(f"✔ Arquivo salvo: {saida_path}")


# ---------------------------------------------------------
# Processamento automático por diretório de centróides
# ---------------------------------------------------------
def processar_todos(animais_csv, centroids_dir, output_dir):

    animais_path = pathlib.Path(animais_csv)
    centroids_path = pathlib.Path(centroids_dir)
    output_path = pathlib.Path(output_dir)

    # IMPORTANTE: animais sem cabeçalho
    df_animais = pd.read_csv(animais_path, header=None)
    print(f"✔ Animais carregados ({len(df_animais)} linhas)")

    centroid_files = list(centroids_path.glob("centroids_*_*_rawdata.csv"))
    if not centroid_files:
        print("❌ Nenhum arquivo de centróides encontrado.")
        return

    for csv_path in centroid_files:
        nome = csv_path.stem  # ex.: centroids_16_birch_rawdata
        partes = nome.split("_")

        n_centroides = partes[1]  # 8, 16, 32
        metodo = partes[2]        # kmeans, birch, som

        saida = output_path / f"distancias_resumo_{metodo}_{n_centroides}_centroides.csv"

        print(f"\n▶ Processando {csv_path.name} ...")

        # centróides também sem cabeçalho
        df_centroids = pd.read_csv(csv_path, header=None)

        calcular_distancias_por_centroides(
            df_animais=df_animais,
            df_centroids=df_centroids,
            metodo=metodo,
            saida_path=saida,
        )

    print("\n✔ Finalizado para todos os arquivos.")


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Cálculo de distâncias entre animais e centróides"
    )

    parser.add_argument(
        "--animals",
        required=True,
        help="Caminho do CSV contendo todos os animais (sem cabeçalho: id, ts, lon, lat)",
    )
    parser.add_argument(
        "--centroids-dir",
        required=True,
        help="Diretório contendo os CSVs dos centróides (sem cabeçalho: id, lon, lat)",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Diretório onde serão salvos os resultados",
    )

    args = parser.parse_args()

    processar_todos(args.animals, args.centroids_dir, args.output_dir)
