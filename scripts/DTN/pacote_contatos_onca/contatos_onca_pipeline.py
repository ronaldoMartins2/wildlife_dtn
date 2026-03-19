# -*- coding: utf-8 -*-
"""
Pipeline para estimar métricas de contato entre onças-pintadas a partir de dados de telemetria (CSV).

Justificativas científicas:
- Threshold de contato d* = 200 m: proximidade entre onças encontrada a <200 m com frequência > ao acaso,
  suporte empírico para tratar como proxy de contato/associação (Cavalcanti & Gese, 2009).
- Detecção de eventos por co-ocorrência espaço-temporal/trajetórias: aplicável a P. onca (Garcia Fontes et al., 2021).
- Durações de contato com cauda longa: distribuições lognormal/Weibull amplamente usadas com sensores de proximidade
  (Plaszczynski et al., 2024), transferíveis para fauna com proximidade recorrente.

Autor: (c) Eliézer Passos & M365 Copilot
"""

import os
import math
import argparse
from itertools import combinations
import numpy as np
import pandas as pd
from scipy.stats import lognorm, weibull_min
import matplotlib.pyplot as plt

CONFIG = {
    "INPUT_CSV": "dados_telemetria.csv",
    "OUTPUT_DIR": r"scripts\DTN\pacote_contatos_onca\output_contatos",
    "COL_ID": "individual.local.identifier (ID)",
    "COL_TIME": "timestamp",
    "COL_LAT": "location.lat",
    "COL_LON": "location.long",
    "COL_SEX": "sex",
    "COL_GROUP": "group",
    "TIMEZONE": "America/Manaus",            # ex.: "America/Manaus"
    "DELTA_T_MIN": 30,
    "NEAREST_TOL_MIN": 15,
    "MIN_FIXES_PER_EVENT": 2,
    "DIST_THRESHOLD_M": 200.0,
    "GAP_TOLERANCE_STEPS": 1,
    "MIN_DURATION_MIN": 10,
    "WINDOW_FOR_SUMMARY_DAYS": 7,
}

# -----------------------------
# Funções auxiliares
# -----------------------------

def ensure_outdir(path):
    os.makedirs(path, exist_ok=True)
    return path


def to_minutes(td):
    return td.total_seconds() / 60.0


def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    phi1 = np.radians(lat1); phi2 = np.radians(lat2)
    dphi = phi2 - phi1
    dl = np.radians(lon2 - lon1)
    a = np.sin(dphi/2.0)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(dl/2.0)**2
    return 2*R*np.arcsin(np.sqrt(a))


def parse_and_prepare(df, cfg):
    df = df.copy()
    df[cfg["COL_TIME"]] = pd.to_datetime(df[cfg["COL_TIME"]], utc=False, errors="coerce")
    if cfg["TIMEZONE"]:
        if df[cfg["COL_TIME"]].dt.tz is None:
            df[cfg["COL_TIME"]] = df[cfg["COL_TIME"]].dt.tz_localize(cfg["TIMEZONE"]) 
        else:
            df[cfg["COL_TIME"]] = df[cfg["COL_TIME"]].dt.tz_convert(cfg["TIMEZONE"]) 

    df = df.sort_values([cfg["COL_ID"], cfg["COL_TIME"]]).dropna(subset=[cfg["COL_LAT"], cfg["COL_LON"]])

    out_list = []
    freq = f'{cfg["DELTA_T_MIN"]}T'
    tol = f'{cfg["NEAREST_TOL_MIN"]}T'
    for pid, g in df.groupby(cfg["COL_ID"]):
        g = g.set_index(cfg["COL_TIME"]).sort_index()
        grid = pd.DataFrame(index=pd.date_range(g.index.min(), g.index.max(), freq=freq))
        g2 = pd.merge_asof(
            left=grid.reset_index().rename(columns={"index": cfg["COL_TIME"]}),
            right=g.reset_index()[[cfg["COL_TIME"], cfg["COL_LAT"], cfg["COL_LON"]]],
            on=cfg["COL_TIME"], direction="nearest", tolerance=pd.Timedelta(tol)
        )
        g2[cfg["COL_ID"]] = pid
        out_list.append(g2)

    df_reg = pd.concat(out_list, ignore_index=True)
    df_reg = df_reg.dropna(subset=[cfg["COL_LAT"], cfg["COL_LON"]])
    df_reg = df_reg.set_index(cfg["COL_TIME"]).sort_index()
    return df_reg


def fill_short_gaps(contact_bool, max_gap):
    s = contact_bool.astype(int).values
    n = len(s); i = 0
    while i < n:
        if s[i] == 1:
            i += 1; continue
        j = i
        while j < n and s[j] == 0:
            j += 1
        gap_len = j - i
        left = (i > 0 and s[i-1] == 1)
        right = (j < n and s[j] == 1)
        if left and right and gap_len <= max_gap:
            s[i:j] = 1
        i = j
    return pd.Series(s, index=contact_bool.index).astype(bool)


def segment_events(contact_bool):
    starts = contact_bool & ~contact_bool.shift(1, fill_value=False)
    event_id = starts.cumsum()
    event_id = event_id.where(contact_bool, other=np.nan)
    return event_id


def aic_from_loglik(loglik, k_params):
    return -2.0*loglik + 2.0*k_params


def fit_lognormal(durations_min):
    x = np.array(durations_min, dtype=float)
    x = x[x > 0]
    sigma, loc, scale = lognorm.fit(x, floc=0)
    ll = np.sum(lognorm.logpdf(x, s=sigma, loc=0, scale=scale))
    k = 2  # mu,sigma
    return {"dist": "lognormal", "sigma": sigma, "mu": math.log(scale), "loglik": ll, "aic": aic_from_loglik(ll, k)}


def fit_weibull(durations_min):
    x = np.array(durations_min, dtype=float)
    x = x[x > 0]
    c, loc, scale = weibull_min.fit(x, floc=0)
    ll = np.sum(weibull_min.logpdf(x, c=c, loc=0, scale=scale))
    k = 2
    return {"dist": "weibull", "shape": c, "scale": scale, "loglik": ll, "aic": aic_from_loglik(ll, k)}


def empirical_survival(x, grid=None):
    x = np.sort(np.asarray(x, dtype=float))
    n = len(x)
    if n == 0:
        return np.array([]), np.array([])
    if grid is None:
        grid = np.linspace(0, x.max(), 200)
    S = np.array([(x > t).mean() for t in grid])
    return grid, S


def main(cfg):
    ensure_outdir(cfg["OUTPUT_DIR"])
    df = pd.read_csv(cfg["INPUT_CSV"])
    # cols_keep = [c for c in [cfg["COL_ID"], cfg["COL_TIME"], cfg["COL_LAT"], cfg["COL_LON"], cfg["COL_SEX"], cfg["COL_GROUP"]] if c in df.columns]
    cols_keep = [c for c in [cfg["COL_ID"], cfg["COL_TIME"], cfg["COL_LAT"], cfg["COL_LON"]] if c in df.columns]
    df = df[cols_keep]

    df_reg = parse_and_prepare(df, cfg)
    individuals = df_reg[cfg["COL_ID"]].unique().tolist(); individuals.sort()
    delta_t = pd.Timedelta(minutes=cfg["DELTA_T_MIN"])

    all_events = []
    dyad_metrics = []
    ids_pairs = list(combinations(individuals, 2))

    meta = df.drop_duplicates(subset=[cfg["COL_ID"]]).set_index(cfg["COL_ID"])
    sex_map = meta[cfg["COL_SEX"]].to_dict() if cfg["COL_SEX"] in meta.columns else {}
    grp_map = meta[cfg["COL_GROUP"]].to_dict() if cfg["COL_GROUP"] in meta.columns else {}

    for id1, id2 in ids_pairs:
        g1 = df_reg[df_reg[cfg["COL_ID"]] == id1][[cfg["COL_LAT"], cfg["COL_LON"]]]
        g2 = df_reg[df_reg[cfg["COL_ID"]] == id2][[cfg["COL_LAT"], cfg["COL_LON"]]]
        idx = g1.index.intersection(g2.index)
        if len(idx) < cfg["MIN_FIXES_PER_EVENT"]:
            continue
        g1c = g1.loc[idx]; g2c = g2.loc[idx]
        dist_m = haversine_m(g1c[cfg["COL_LAT"]].values, g1c[cfg["COL_LON"]].values,
                             g2c[cfg["COL_LAT"]].values, g2c[cfg["COL_LON"]].values)
        sdist = pd.Series(dist_m, index=idx)
        contact_raw = sdist <= cfg["DIST_THRESHOLD_M"]
        contact = fill_short_gaps(contact_raw, cfg["GAP_TOLERANCE_STEPS"])
        if not contact.any():
            continue
        ev_id = segment_events(contact)
        events = []
        for eid, seg in ev_id.dropna().groupby(ev_id.dropna()):
            mask = (ev_id == eid) & contact
            if not mask.any():
                continue
            t_start = mask.index[mask.argmax()]
            t_end = mask.index[mask[::-1].argmax()]
            n_steps = int(mask.sum())
            duration_min = n_steps * cfg["DELTA_T_MIN"]
            if duration_min < cfg["MIN_DURATION_MIN"] or n_steps < cfg["MIN_FIXES_PER_EVENT"]:
                continue
            events.append({"id1": id1, "id2": id2, "t_start": t_start, "t_end": t_end, "duration_min": duration_min})
        if not events:
            continue
        ev_df = pd.DataFrame(events)
        all_events.append(ev_df)

        n_events = len(ev_df)
        total_minutes = ev_df["duration_min"].sum()
        mean_min = ev_df["duration_min"].mean()
        median_min = ev_df["duration_min"].median()
        p95_min = ev_df["duration_min"].quantile(0.95)
        tmin, tmax = idx.min(), idx.max()
        days = (tmax - tmin) / pd.Timedelta(days=1)
        lambda_day = n_events / days if days > 0 else np.nan
        ev_df_sorted = ev_df.sort_values("t_start")
        if len(ev_df_sorted) > 1:
            ict = ev_df_sorted["t_start"].iloc[1:].values - ev_df_sorted["t_end"].iloc[:-1].values
            ict_min = [to_minutes(pd.to_timedelta(x)) for x in ict]
        else:
            ict_min = []
        dyad_metrics.append({
            "id1": id1, "id2": id2,
            "sex1": sex_map.get(id1, None), "sex2": sex_map.get(id2, None),
            "group1": grp_map.get(id1, None), "group2": grp_map.get(id2, None),
            "n_events": n_events,
            "monitoring_days_common": days,
            "lambda_events_per_day": lambda_day,
            "total_contact_minutes": total_minutes,
            "mean_duration_min": mean_min,
            "median_duration_min": median_min,
            "p95_duration_min": p95_min,
            "ict_count": len(ict_min),
            "ict_median_min": np.median(ict_min) if len(ict_min) > 0 else np.nan
        })

    if len(all_events) == 0:
        print("Nenhum evento de contato detectado com os parâmetros atuais.")
        return

    events_all = pd.concat(all_events, ignore_index=True)
    out_dir = ensure_outdir(cfg["OUTPUT_DIR"])
    events_all.to_csv(os.path.join(out_dir, "eventos_contato.csv"), index=False)
    dyad_df = pd.DataFrame(dyad_metrics)
    dyad_df.to_csv(os.path.join(out_dir, "metricas_por_dyade.csv"), index=False)

    durations = events_all["duration_min"].values
    fit_ln = fit_lognormal(durations)
    fit_wb = fit_weibull(durations)
    fits = pd.DataFrame([fit_ln, fit_wb])
    fits.to_csv(os.path.join(out_dir, "ajuste_distribuicoes.csv"), index=False)
    best = fits.sort_values("aic").iloc[0].to_dict()
    print("Melhor ajuste (AIC):", best)

    # Gráficos
    plt.figure(figsize=(7, 4))
    bins = 30
    mx = max(180, durations.max())
    xs = np.linspace(1, mx, 400)
    plt.hist(durations, bins=bins, density=True, alpha=0.35, color="#2c7fb8", label="Dados (durações)")
    pdf_ln = lognorm.pdf(xs, s=fit_ln["sigma"], scale=np.exp(fit_ln["mu"]))
    plt.plot(xs, pdf_ln, color="#08306b", lw=2, label="Lognormal (ajuste)")
    pdf_wb = weibull_min.pdf(xs, c=fit_wb["shape"], scale=fit_wb["scale"])
    plt.plot(xs, pdf_wb, color="#b30059", lw=2, ls="--", label="Weibull (ajuste)")
    plt.title("Distribuição das durações de contato (min)")
    plt.xlabel("Duração (min)")
    plt.ylabel("Densidade")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "duracoes_hist_ajustes.png"), dpi=160)
    plt.close()

    grid, S_emp = empirical_survival(durations, grid=np.linspace(0, max(6*60, durations.max()), 300))
    S_ln = 1 - lognorm.cdf(grid, s=fit_ln["sigma"], scale=np.exp(fit_ln["mu"]))
    S_wb = 1 - weibull_min.cdf(grid, c=fit_wb["shape"], scale=fit_wb["scale"])
    plt.figure(figsize=(7, 4))
    plt.plot(grid, S_emp, color="#0868ac", lw=2, label="Empírica")
    plt.plot(grid, S_ln, color="#08306b", lw=2, ls="-.", label="Lognormal (ajuste)")
    plt.plot(grid, S_wb, color="#b30059", lw=2, ls="--", label="Weibull (ajuste)")
    plt.yscale("log")
    plt.title("Função de sobrevivência (1−CDF) — cauda longa")
    plt.xlabel("t (min)")
    plt.ylabel("P(Duração > t) [escala log]")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "sobrevivencia_emp_ajustes.png"), dpi=160)
    plt.close()

    ids_all = sorted(set(dyad_df["id1"]).union(set(dyad_df["id2"])))
    mat = pd.DataFrame(0.0, index=ids_all, columns=ids_all)
    for _, r in dyad_df.iterrows():
        mat.loc[r["id1"], r["id2"]] = r["total_contact_minutes"]
        mat.loc[r["id2"], r["id1"]] = r["total_contact_minutes"]
    plt.figure(figsize=(6, 5))
    im = plt.imshow(mat.values, cmap="YlGnBu")
    plt.colorbar(im, label="Tempo total de contato (min)")
    plt.xticks(ticks=np.arange(len(ids_all)), labels=ids_all, rotation=90)
    plt.yticks(ticks=np.arange(len(ids_all)), labels=ids_all)
    plt.title("Heatmap — tempo total de contato por díade")
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "heatmap_tempo_por_dyade.png"), dpi=160)
    plt.close()
    mat.to_csv(os.path.join(out_dir, "matriz_tempo_contato_min.csv"))

    w_days = cfg["WINDOW_FOR_SUMMARY_DAYS"]
    events_all["week"] = pd.to_datetime(events_all["t_start"]).dt.to_period(f"{w_days}D").astype(str)
    weekly = (events_all.groupby(["id1", "id2", "week"])["duration_min"].sum().reset_index().rename(columns={"duration_min": f"total_min_{w_days}d"}))
    weekly.to_csv(os.path.join(out_dir, f"contato_por_janela_{w_days}d.csv"), index=False)

    print("Concluído. Arquivos salvos em:", out_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline de métricas de contato para onças-pintadas (CSV telemetria).")
    parser.add_argument("--input", type=str, default=CONFIG["INPUT_CSV"], help="Caminho do CSV de entrada.")
    parser.add_argument("--out", type=str, default=CONFIG["OUTPUT_DIR"], help="Diretório de saída.")
    parser.add_argument("--dt", type=int, default=CONFIG["DELTA_T_MIN"], help="Δt de reamostragem (min).")
    parser.add_argument("--tol", type=int, default=CONFIG["NEAREST_TOL_MIN"], help="Tolerância p/ Pareamento (min).")
    parser.add_argument("--dstar", type=float, default=CONFIG["DIST_THRESHOLD_M"], help="Threshold de contato d* (m).")
    parser.add_argument("--gap", type=int, default=CONFIG["GAP_TOLERANCE_STEPS"], help="Lacuna máxima (passos) para fechar dentro de um evento.")
    parser.add_argument("--mindur", type=int, default=CONFIG["MIN_DURATION_MIN"], help="Duração mínima para aceitar evento (min).")
    args = parser.parse_args()

    CONFIG["INPUT_CSV"] = args.input
    CONFIG["OUTPUT_DIR"] = args.out
    CONFIG["DELTA_T_MIN"] = args.dt
    CONFIG["NEAREST_TOL_MIN"] = args.tol
    CONFIG["DIST_THRESHOLD_M"] = args.dstar
    CONFIG["GAP_TOLERANCE_STEPS"] = args.gap
    CONFIG["MIN_DURATION_MIN"] = args.mindur

    main(CONFIG)
