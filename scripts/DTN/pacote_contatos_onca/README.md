# Pacote — Contatos de Onça-pintada (Panthera onca)

Este pacote contém:

- `contatos_onca_pipeline.py`: script completo para detectar eventos de contato, calcular métricas por díade e ajustar distribuições.
- `templates_graficos/abnt_figuras.mplstyle`: estilo Matplotlib para figuras acadêmicas.
- `templates_graficos/exemplo_template_grafico.py`: exemplo simples de gráfico usando as saídas do script.

## Uso rápido

1. Prepare um CSV com colunas: `id,timestamp,lat,lon[,sex][,group]`.
2. Execute o script:
   ```bash
   python contatos_onca_pipeline.py --input dados_telemetria.csv --out resultados_onca --dt 30 --tol 15 --dstar 200 --gap 1 --mindur 10
   ```
3. Rode o template opcional (após o passo 2, pois ele depende de `output_contatos/eventos_contato.csv` ou ajuste o caminho se mudou `--out`):
   ```bash
   python templates_graficos/exemplo_template_grafico.py
   ```

## Justificativas metodológicas
- **d* = 200 m**: proximidade entre onças observada com frequência acima do acaso (Cavalcanti & Gese, 2009) — proxy de contato.
- **Co-ocorrência/trajectórias** em *P. onca* para identificar interações (Garcia Fontes et al., 2021).
- **Durações com cauda longa**: uso de Lognormal/Weibull (Plaszczynski et al., 2024; EPJ Data Science e arXiv).

## Saídas principais
- `eventos_contato.csv`, `metricas_por_dyade.csv`, `ajuste_distribuicoes.csv`
- Figuras: `duracoes_hist_ajustes.png`, `sobrevivencia_emp_ajustes.png`, `heatmap_tempo_por_dyade.png`

## Observações
- Faça análise de sensibilidade para `--dstar` (100–300 m), `--dt` e `--gap`.
- Garanta que timestamps estejam no mesmo fuso e bem sincronizados por indivíduo.
