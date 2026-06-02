# Métricas de Qualidade para Interpolação de Trajetórias de Vida Selvagem

## Resumo das Métricas Adicionadas

Este documento descreve as novas métricas biológicas e ecológicas adicionadas ao pipeline de avaliação de interpolação para N-BEATS e BiLSTM.

---

## 1. Métricas Geométricas Mantidas

### ADE (Average Displacement Error) - em metros
- **O que mede**: Erro médio de posição em toda a trajetória
- **Interpretação**: Quanto, em média, a interpolação está afastada da posição real
- **Ideal**: Valores menores são melhores
- **Contexto de onça**: ~200-500m é aceitável (considerando erros de GPS)

### FDE (Final Displacement Error) - em metros
- **O que mede**: Erro acumulado no final da trajetória
- **Interpretação**: Detecta "drift" (desvio) em predições multi-step
- **Ideal**: Valores menores são melhores
- **Aviso**: Se FDE >> ADE, o modelo perdeu o rastro ao longo do tempo

### RMSE_deltas e MAE_deltas (Erro nos Deslocamentos)
- **O que medem**: Erro na magnitude e direção dos "passos" entre pontos
- **Interpretação**: Se o modelo aprendeu a inércia corretamente
- **Ideal**: Valores menores indicam deslocamentos mais realistas
- **Contexto**: Importante para detectar sobre-suavização (over-smoothing)

---

## 2. Novas Métricas Biológicas Adicionadas

### 1. infeasible_steps_ratio (Taxa de Passos Inviáveis)

**Arquivo**: `Evaluation/biological_metrics.py`  
**Função**: `calculate_infeasible_steps_ratio()`

#### O que mede
Porcentagem de passos interpolados que implicam velocidades biologicamente impossíveis para a espécie.

#### Por que importa
- Redes neurais (especialmente LSTM) tendem a suavizar trajetórias e criar "atalhos"
- Uma onça não pode correr a 90 km/h continuamente
- GPS falha sob dossel denso, mas interpolações devem ser realistas

#### Limites de Velocidade por Espécie (implementados)
- **Jaguar**: 25 m/s (90 km/h) - máximo sustentável em trânsito
- **Puma**: 20 m/s (72 km/h)
- **Ocelot**: 15 m/s (54 km/h)
- **Pássaros**: 20 m/s (72 km/h)

#### Interpretação
```
Razão = 0.0       → Perfeito! Todos os passos são viáveis
Razão = 0.05      → Excelente (5% de passos inviáveis)
Razão = 0.15      → Aceitável (15%)
Razão = 0.30+     → Problemático (30%+) - interpolação muito suave ou com saltos
```

#### Exemplo de Uso
```python
from Evaluation.biological_metrics import calculate_infeasible_steps_ratio

ratio, details = calculate_infeasible_steps_ratio(
    lons_true, lats_true, times_true,
    lons_pred, lats_pred, times_pred,
    species='jaguar',
    return_details=True
)
print(f"Taxa inviável: {ratio:.2%}")
print(f"Velocidade máxima observada: {details['max_speed_observed']:.1f} m/s")
```

---

### 2. turning_angles_kl_divergence (Divergência de Ângulos de Giro)

**Arquivo**: `Evaluation/biological_metrics.py`  
**Função**: `calculate_turning_angles_kl_divergence()`

#### O que mede
Diferença entre a distribuição de ângulos de giro (turning angles) na trajetória real vs predita.

#### Por que importa
- Onças patrulham, ziguezagueiam perseguindo presas
- Redes LSTM simplistas podem produzir trajetórias muito retas
- A "assinatura de movimento" reflete o comportamento animal

#### Ângulo de Giro (Turning Angle)
Ângulo formado por 3 pontos consecutivos:
```
      ponto[i+1]
         /
        /θ (turning angle)
       /
    ponto[i]
```

#### Interpretação
```
KL divergence = 0.0      → Distribuições idênticas (perfeito!)
KL divergence < 0.50     → Muito semelhante (excelente)
KL divergence < 1.0      → Semelhante (bom)
KL divergence < 2.0      → Moderadamente diferente
KL divergence > 2.0      → Muito diferente (problema!)
```

#### O que significa cada KL baixo/alto
- **KL baixo**: Interpolação mantém padrões de movimento da espécie
- **KL alto**: Trajetória predita é muito diferente (muito reta, muito zigzagueante, etc)

#### Exemplo de Uso
```python
from Evaluation.biological_metrics import calculate_turning_angles_kl_divergence

kl_div, details = calculate_turning_angles_kl_divergence(
    lons_true, lats_true,
    lons_pred, lats_pred,
    n_bins=36,  # 10-degree bins
    return_details=True
)
print(f"KL Divergência: {kl_div:.3f}")
print(f"Ângulo médio real: {details['mean_angle_true']:.1f}°")
print(f"Ângulo médio predito: {details['mean_angle_pred']:.1f}°")
```

---

### 3. dtw_distance_normalized (Dynamic Time Warping)

**Arquivo**: `Evaluation/biological_metrics.py`  
**Função**: `calculate_dtw_distance()`

#### O que mede
Distância espacial mínima entre as trajetórias, ignorando pequenas dessincronizações temporais.

#### Por que importa
- Às vezes a interpolação acerta a forma do caminho mas com um pequeno atraso temporal
- ADE/FDE punem severamente esses casos
- DTW reconhece que a **forma** está correta mesmo se o tempo está um pouco fora

#### Contexto de Uso
```
Cenário real:
- Trajetória real: [A] → [B] → [C]
- Interpolação: [A'] → [A''] → [B] → [C]  (mais pontos, mesmo caminho)

ADE: Muito alto (porque A'' não corresponde a B temporalmente)
DTW: Muito baixo (porque a forma do caminho é idêntica)
```

#### Interpretação
```
DTW normalizado (metros/passo):
- DTW < 100m    → Trajetos muito semelhantes
- DTW < 300m    → Trajetos semelhantes
- DTW < 500m    → Trajetos moderadamente semelhantes
- DTW > 1000m   → Trajetos muito diferentes
```

#### Exemplo de Uso
```python
from Evaluation.biological_metrics import calculate_dtw_distance

dtw_dist = calculate_dtw_distance(
    lons_true, lats_true,
    lons_pred, lats_pred,
    normalize=True
)
print(f"DTW normalizado: {dtw_dist:.1f} metros/passo")
```

**Dependência**: Requer `dtaidistance` (instalado automaticamente via requirements.txt)

---

### 4. sinuosity (Sinuosidade/Tortuosidade)

**Arquivo**: `Evaluation/biological_metrics.py`  
**Função**: `calculate_sinuosity()`

#### O que mede
Razão entre o comprimento do caminho percorrido vs distância reta entre início e fim.

#### Interpretação
```
Sinuosidade = 1.0       → Linha reta (improvável em natureza)
Sinuosidade = 1.2-1.5   → Caminho moderadamente tortuoso (típico)
Sinuosidade = 2.0-3.0   → Muito tortuoso (patrulha intensa)
Sinuosidade > 5.0       → Extremamente tortuoso
```

#### Métrica Derivada: sinuosity_ratio
```
Razão = Sinuosidade_predita / Sinuosidade_real

Razão = 1.0    → Interpolação mantém sinuosidade
Razão < 1.0    → Muito reta (over-smoothing)
Razão > 1.0    → Muito tortuosa (sobre-predição de movimento)
```

#### Exemplo de Uso
```python
sin_real = calculate_sinuosity(lons_true, lats_true)
sin_pred = calculate_sinuosity(lons_pred, lats_pred)
ratio = sin_pred / sin_real

print(f"Sinuosidade real: {sin_real:.2f}")
print(f"Sinuosidade predita: {sin_pred:.2f}")
print(f"Razão (ideal ≈ 1.0): {ratio:.2f}")
```

---

### 5. frechet_distance (Distância de Fréchet)

**Arquivo**: `Evaluation/biological_metrics.py`  
**Função**: `calculate_frechet_distance()`

#### O que mede
Distância mínima que um "cachorro" ligado por uma corda deve viajar para ir de uma trajetória para outra.

#### Por que importa
- Mede similaridade de forma de trajetórias de forma robusta
- Diferente de DTW: não requer correspondência ponto-a-ponto

#### Interpretação
```
Valores em metros:
- Fréchet < 200m   → Trajetórias muito semelhantes em forma
- Fréchet < 500m   → Trajetórias semelhantes
- Fréchet > 1000m  → Trajetórias muito diferentes
```

---

### 6. area_difference_ratio (Diferença de Área Coberta)

**Arquivo**: `Evaluation/biological_metrics.py`  
**Função**: `calculate_area_difference()`

#### O que mede
Razão da diferença na área coberta pelo polígono da trajetória.

#### Interpretação
```
Razão = 0.0        → Mesma área coberta (ideal)
Razão = 0.2        → Interpolação cobre 20% a mais
Razão = -0.3       → Interpolação cobre 30% a menos
```

#### Por que importa
- Detecta se o animal explora mais ou menos espaço na interpolação
- Útil para análises de uso de habitat

---

## 3. Saída dos Resultados

### Estrutura de Diretórios

#### Para N-BEATS
```
scripts/Interpolation/models/
├── nbeats_eval_{filename}.json           # Métricas por horizonte
├── nbeats_eval_summary_{filename}.csv    # Resumo por horizonte
└── nbeats_biological_metrics_{filename}.json  # ✅ NOVO: Métricas biológicas
```

#### Para BiLSTM
```
Results/{dataset}/Interpolation/
├── metrics_bilstm_{animal}.json            # ✅ Atualizado: inclui bio metrics
└── bilstm_eval_summary_{base}.csv          # ✅ Atualizado: inclui bio metrics
```

### Estrutura do JSON de Métricas Biológicas

```json
{
  "filename": "jaguar_mamiraua",
  "ade_meters": 234.5,
  "fde_meters": 512.3,
  "biological_metrics": {
    "infeasible_steps_ratio": 0.08,
    "turning_angles_kl_divergence": 0.45,
    "dtw_distance_normalized": 150.2,
    "sinuosity_true": 1.45,
    "sinuosity_pred": 1.52,
    "sinuosity_ratio": 1.048,
    "area_difference_ratio": 0.120,
    "frechet_distance_meters": 320.5,
    "infeasible_steps_details": {
      "speeds": [...],
      "threshold": 25.0,
      "max_speed_observed": 35.2,
      "mean_speed": 8.5
    },
    "turning_angles_details": {
      "angles_true": [...],
      "angles_pred": [...],
      "mean_angle_true": -2.3,
      "mean_angle_pred": 0.8,
      "std_angle_true": 45.2,
      "std_angle_pred": 42.8
    }
  }
}
```

---

## 4. Guia de Interpretação Consolidado

### Cenário 1: Interpolação Excelente
```
Sinais de sucesso:
✅ ADE < 300m
✅ FDE < 600m
✅ infeasible_steps_ratio < 5%
✅ turning_angles_kl < 0.5
✅ sinuosity_ratio ≈ 1.0 (0.95-1.05)
✅ DTW < 200m
```

### Cenário 2: Over-Smoothing (problema comum)
```
Sinais de alerta:
❌ ADE razoável mas FDE muito alto (drift)
❌ sinuosity_ratio << 1.0 (trajetória muito reta)
❌ turning_angles_kl muito baixa (muito linear)
⚠️ Solução: Aumentar regularização ou ajustar arquitetura
```

### Cenário 3: Saltos Irrealistas
```
Sinais de alerta:
❌ infeasible_steps_ratio > 20%
❌ Velocidades máximas > limites biológicos
❌ DTW e Fréchet muito altos
⚠️ Solução: Adicionar constraints de velocidade no modelo
```

### Cenário 4: Trajetória Muito Tortuosa
```
Sinais de alerta:
❌ sinuosity_ratio >> 1.0 (muito tortuosa)
❌ DTW: baixo mas sinuosidade diferente
⚠️ Pode indicar: Ruído sendo amplificado ou padrão real (verificar visualmente)
```

---

## 5. Como Usar as Métricas no Seu Código

### Uso Direto (para desenvolvimento)
```python
import numpy as np
from Evaluation.biological_metrics import compute_all_biological_metrics

# Suas trajetórias
lons_true = np.array([...])  # Coordenadas reais (graus)
lats_true = np.array([...])
times_true = np.array([...])  # Timestamps em qualquer unidade consistente

lons_pred = np.array([...])  # Coordenadas preditas
lats_pred = np.array([...])
times_pred = np.array([...])

# Calcular todas as métricas de uma vez
metrics = compute_all_biological_metrics(
    lons_true, lats_true, times_true,
    lons_pred, lats_pred, times_pred,
    species='jaguar'
)

# Acessar resultados
print(f"Taxa de passos inviáveis: {metrics['infeasible_steps_ratio']:.2%}")
print(f"KL divergência dos ângulos: {metrics['turning_angles_kl_divergence']:.3f}")
print(f"DTW normalizado: {metrics['dtw_distance_normalized']:.1f}m")
print(f"Sinuosidade: Real={metrics['sinuosity_true']:.2f}, Predita={metrics['sinuosity_pred']:.2f}")
```

### Uso Individual (métricas específicas)
```python
from Evaluation.biological_metrics import (
    calculate_infeasible_steps_ratio,
    calculate_turning_angles_kl_divergence,
    calculate_dtw_distance
)

# Apenas velocidade
infeasible_ratio = calculate_infeasible_steps_ratio(
    lons_true, lats_true, times_true,
    lons_pred, lats_pred, times_pred,
    species='jaguar'
)

# Apenas ângulos de giro
kl_div = calculate_turning_angles_kl_divergence(
    lons_true, lats_true,
    lons_pred, lats_pred
)

# Apenas DTW
dtw = calculate_dtw_distance(
    lons_true, lats_true,
    lons_pred, lats_pred,
    normalize=True
)
```

---

## 6. Dependências Adicionadas

```
scipy      # Para KL divergence e estatísticas
dtaidistance  # Para DTW otimizado
```

Ambas estão em `scripts/requirements.txt` e serão instaladas automaticamente.

---

## 7. Próximos Passos Sugeridos

1. **Executar avaliação completa**
   ```bash
   cd /home/abinadabe/projetos/wildlife_dtn
   source venv/bin/activate
   python scripts/Interpolation/evaluate_nbeats.py <caminho_mapa> <arquivo_bruto> <colunas_json>
   python scripts/Interpolation/evaluate_bilstm.py <arquivo_bruto> <colunas_json>
   ```

2. **Visualizar métricas biológicas**: Criar gráficos dos resultados JSON

3. **Ajustar modelos**: Usar as métricas para otimizar N-BEATS e BiLSTM

4. **Comparação entre métodos**: Usar todas as métricas para comparação justa entre interpolações

---

## 8. Referências

- **DTW**: Dynamic Time Warping - Distance Measure for Time Series Data
- **Frechet Distance**: Continuous Analogues of Polygonal Curves
- **KL Divergence**: Information Theory perspective on distribution differences
- **Sinuosity**: Standard ecological metric for movement patterns

---

## Contato / Dúvidas

Para dúvidas sobre uso ou interpretação das métricas, consulte a docstring detalhada em `Evaluation/biological_metrics.py`.
