
# 🐆 Jaguar Data Analysis Project

Este projeto realiza a análise, previsão e agrupamento de dados de localização de onças, com foco em séries temporais, redes neurais MLP e algoritmos de clusterização. Está estruturado em módulos claros e reutilizáveis para facilitar o processamento de dados e a extração de padrões de comportamento.

---

## 📁 Estrutura de Pastas

```
scripts/
├── jaguar_mamiraua.csv               # Arquivo principal de dados
├── requirements.txt                  # Requisitos do projeto
├── MLP_model.py                      # Treinamento de rede neural MLP
├── MLP_forecast.py                   # Previsão usando modelo MLP treinado
├── Clusterization/                   # Algoritmos de clusterização e visualizações
│   ├── BIRCH.py
│   ├── kmeans_individual_csv.py
│   ├── Mean_Shift.py
│   ├── onca_contacts.py
│   ├── plot_kmeans_som.py
│   ├── plot_kmeans_som_birch_mean_shift.py
│   ├── SOM_individual.py
│   └── __init__.py
├── Common/                           # Scripts de pré-processamento e utilitários
│   ├── 1_separar_localizacoes_individuais_to_plot.py
│   ├── 2_encontrar_contatos_entre_nos.py
│   ├── 2_encontrar_contatos_entre_no_fixo_movel.py
│   ├── 3_adicionar_evento_down.py
│   ├── utils.py
│   └── __init__.py
```

---

## 🧪 Instalação

### 1. Clone o repositório (ou extraia o zip)

```bash
git clone <url>
cd scripts
```

### 2. Crie um ambiente virtual

```bash
python -m venv venv
source venv/bin/activate       # Linux/Mac
venv\Scripts\activate        # Windows
```

### 3. Instale as dependências

```bash
pip install -r requirements.txt
```

---

## 📊 Dataset

O arquivo `jaguar_mamiraua.csv` contém os dados de localização de onças, com campos como:

- ID do animal
- Latitude e Longitude
- Data e hora
- Eventos ou status (como "down")

---

## 🧠 Modelagem Preditiva - MLP

### `scripts/MLP_model.py`

- Realiza o treinamento de uma rede neural MLP.
- Usa uma janela deslizante sobre as séries temporais.

**Execução:**

```bash
python MLP_model.py
```

### `scripts/MLP_forecast.py`

- Usa o modelo treinado para realizar previsões.

**Execução:**

```bash
python MLP_forecast.py
```

---

## 🧭 Clusterização

Scripts localizados em `scripts/Clusterization/`:

| Script                                 | Descrição |
|----------------------------------------|-----------|
| `kmeans_individual_csv.py`             | K-Means por indivíduo |
| `BIRCH.py`                             | Algoritmo BIRCH |
| `Mean_Shift.py`                        | Mean Shift |
| `SOM_individual.py`                    | Self-Organizing Maps |
| `plot_kmeans_som.py`                   | Plota resultados de K-Means e SOM |
| `plot_kmeans_som_birch_mean_shift.py` | Compara visualmente os métodos |
| `onca_contacts.py`                     | Contatos entre onças via clusters |

**Exemplo de Execução:**

```bash
python Clusterization/kmeans_individual_csv.py
python Clusterization/plot_kmeans_som_birch_mean_shift.py
```

---

## ⚙️ Pré-processamento e Utilidades

Scripts em `scripts/Common/`:

| Script                                           | Função |
|--------------------------------------------------|--------|
| `1_separar_localizacoes_individuais_to_plot.py`  | Separa dados por animal |
| `2_encontrar_contatos_entre_nos.py`              | Contatos entre animais |
| `2_encontrar_contatos_entre_no_fixo_movel.py`    | Contato nó fixo x móvel |
| `3_adicionar_evento_down.py`                     | Adiciona eventos de desconexão |
| `utils.py`                                       | Funções auxiliares |

**Execução Recomendada:**

```bash
python Common/1_separar_localizacoes_individuais_to_plot.py
python Common/2_encontrar_contatos_entre_nos.py
```

---

## 🔁 Fluxo Recomendado

```bash
# 1. Pré-processamento
python Common/1_separar_localizacoes_individuais_to_plot.py
python Common/2_encontrar_contatos_entre_nos.py

# 2. Modelagem com MLP
python MLP_model.py
python MLP_forecast.py

# 3. Clusterização
python Clusterization/kmeans_individual_csv.py
python Clusterization/plot_kmeans_som_birch_mean_shift.py
```

