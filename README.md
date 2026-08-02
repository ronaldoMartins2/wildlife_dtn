# 🐆 Wildlife DTN — Processamento, Interpolação e Análise de Trajetórias da Fauna

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)

O **Wildlife DTN** é uma plataforma modular desenvolvida para pré-processamento, interpolação de falhas de sinal, previsão de movimentação, clusterização espacial e avaliação biológica de trajetórias de animais (como onças monitoradas via colares GPS) em redes tolerantes a atrasos e interrupções (DTN — *Delay-Tolerant Networks*).

---

## 📌 Principais Funcionalidades

- **📈 Interpolação de Trajetórias (Deep Learning)**: Preenchimento de lacunas e falhas de amostragem em séries temporais de GPS com **BiLSTM** e **N-BEATS**.
- **🔮 Previsão de Movimento**: Modelos preditivos de localização utilizando redes neurais recorrentes (**RNN**, **LSTM**, **GRU**, **MLP**).
- **🧭 Clusterização & Comportamento Espacial**: Identificação de zonas de permanência e contatos utilizando **K-Means**, **BIRCH** e **SOM** (*Self-Organizing Maps*).
- **📡 Simulação DTN & Análise de Contatos**: Algoritmos para detecção de interações/contatos entre nós móveis (fauna) e estações/nós fixos.
- **🧬 Avaliação de Métricas Espaciais e Biológicas**: Avaliação da fidelidade das trajetórias através de métricas de erro (**MSE**, **RMSE**, **ADE**, **FDE**) e métricas biológico-ecológicas (ângulos de viragem, sinuosidade, velocidade e divergência KL).
- **🖥️ Dashboard Interativo (Streamlit)**: Interface gráfica para visualização interativa de dados de localização e resultados.

---

## 📁 Estrutura do Projeto

```text
.
├── rawdata/
│   └── jaguar_mamiraua.csv                 # Dataset de localizações GPS de onças-pintadas
├── scripts/
│   ├── BiLSTM/                              # Pré-processamento e treinamento de modelos BiLSTM
│   │   ├── preprocess_data.py
│   │   └── train_BiLSTM.py
│   ├── Clusterization/                      # Algoritmos de clusterização e dispersão espacial
│   │   ├── BIRCH.py
│   │   ├── SOM_individual.py
│   │   ├── kmeans_individual_csv.py
│   │   ├── onca_contacts.py
│   │   ├── plot_dispersion_geral.py
│   │   └── plot_kmeans_som_birch_mean_shift.py
│   ├── Common/                              # Utilitários e manipuladores de trajetórias/contatos
│   │   ├── 1_separar_localizacoes_individuais_to_plot.py
│   │   ├── 2_encontrar_contatos_entre_nos.py
│   │   └── utils.py
│   ├── Data_preparation/                    # Limpeza de outliers e estruturação dos dados
│   │   ├── clear_outtliers.py
│   │   ├── raw_data_integration.py
│   │   └── separar_localizacoes_individuais.py
│   ├── DTN/                                 # Algoritmos de mobilidade e contatos para redes DTN
│   │   ├── add_down_event.py
│   │   ├── cluster_contacts_fixed_points.py
│   │   ├── find_contacts_between_nodes.py
│   │   └── mobility_contacts.py
│   ├── Evaluation/                          # Cálculo de métricas espaciais e ecológicas
│   │   ├── BIOLOGICAL_METRICS_GUIDE.md
│   │   ├── biological_metrics.py
│   │   ├── average_by_individual.py
│   │   └── metrics.py
│   ├── Interpolation/                       # Modelos de interpolação (BiLSTM, N-BEATS, N-HiTS)
│   │   ├── bilstm_interpolation.py
│   │   ├── evaluate_all_nbeats.py
│   │   ├── evaluate_bilstm.py
│   │   ├── evaluate_nbeats.py
│   │   ├── nbeat_interpolation.py
│   │   ├── nbeat_trainer.py
│   │   └── nhits_interpolation.py
│   ├── Prediction/                          # Modelos de previsão temporal
│   │   ├── 10_RNN_LSTM.py
│   │   └── 9_RNN_GRU.py
│   ├── Wildlife/                            # Interface gráfica web (Streamlit)
│   │   └── app_wildlife.py
│   ├── MLP_forecast.py                      # Previsão baseada em Perceptron Multicamadas
│   ├── MLP_model.py                         # Estrutura do modelo MLP
│   └── requirements.txt                     # Dependências do projeto Python
├── docker-compose.yml                       # Configuração para execução via Docker
└── README.md
```

---

## 🛠️ Instalação e Configuração

### 1. Clonar o repositório
```bash
git clone <url-do-repositorio>
cd wildlife_dtn
```

### 2. Criar e ativar um ambiente virtual

**No Linux/macOS:**
```bash
python3 -m venv venv
source venv/bin/activate
```

**No Windows (PowerShell):**
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

### 3. Instalar as dependências
```bash
pip install -r scripts/requirements.txt
```

---

## 🚀 Como Utilizar

### 🖥️ 1. Iniciação da Aplicação
Para iniciar a aplicação:
```bash
python3 scripts/Wildlife/app_wildlife.py <arquivo.csv> <arquivo_colunas.json>
```

---

### ⚙️ 2. Pré-processamento de Dados
Separação de localizações por indivíduo e remoção de outliers:
```bash
python scripts/Data_preparation/separar_localizacoes_individuais.py
python scripts/Data_preparation/clear_outtliers.py
```

---

### 📈 3. Interpolação de Trajetórias (Missing Data)

#### BiLSTM:
```bash
python scripts/Interpolation/bilstm_interpolation.py
python scripts/Interpolation/evaluate_bilstm.py
```

#### N-BEATS:
```bash
python scripts/Interpolation/run_train_nbeats.py
python scripts/Interpolation/evaluate_nbeats.py
```

---

### 🔮 4. Previsão de Movimentos
Treinamento e execução de modelos recorrentes e MLP:
```bash
# Modelos RNN / GRU / LSTM
python scripts/Prediction/10_RNN_LSTM.py
python scripts/Prediction/9_RNN_GRU.py

# Modelo MLP
python scripts/MLP_forecast.py
```

---

### 🧭 5. Clusterização & Análise de Contatos DTN

#### Algoritmos de Clusterização Espacial:
```bash
python scripts/Clusterization/kmeans_individual_csv.py
python scripts/Clusterization/BIRCH.py
python scripts/Clusterization/SOM_individual.py
```

#### Análise de Mobilidade e Contatos DTN:
```bash
python scripts/DTN/find_contacts_between_nodes.py
python scripts/DTN/cluster_contacts_fixed_points.py
```

---

### 🧬 6. Avaliação e Métricas Biológicas
Para comparar a fidelidade das interpolações com métricas ecológicas (sinuosidade, ângulos de viragem, velocidade, ADE, FDE):
```bash
python scripts/Evaluation/biological_metrics.py
python scripts/Evaluation/average_by_individual.py
```
*(Consulte `scripts/Evaluation/BIOLOGICAL_METRICS_GUIDE.md` para detalhes teóricos sobre as métricas).*

---

## 📊 Dataset

O arquivo base `rawdata/jaguar_mamiraua.csv` contém registros de telemetria GPS da fauna monitorada, incluindo as seguintes variáveis principais:
- **ID / Animal**: Identificador único do indivíduo.
- **Timestamp / DataHora**: Registro temporal da amostragem GPS.
- **Latitude & Longitude**: Coordenadas geográficas.
- **Status / Eventos**: Informações de status do transmissor/conector (ex: eventos "down").

---

## 📄 Licença

Este projeto é desenvolvido para fins acadêmicos e de pesquisa em redes DTN e ecologia de movimento.