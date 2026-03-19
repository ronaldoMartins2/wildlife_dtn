# Pipeline de Simulação de Vida Selvagem e DTN

Este documento descreve o pipeline completo do projeto, desde a preparação dos dados brutos de GPS de animais até a geração de traços de contato para simulação em redes tolerantes a atrasos (DTN).

O script principal que orquestra a execução de todo o pipeline é o `scripts/Wildlife/app_wildlife.py`.

## 1. Configuração do Ambiente

Antes de executar o pipeline, é necessário configurar o ambiente Python.

1.  **Criar um ambiente virtual:**
    ```bash
    python3 -m venv venv
    ```

2.  **Ativar o ambiente virtual:**
    *   No Linux/macOS:
        ```bash
        source ./venv/bin/activate
        ```
    *   No Windows:
        ```bash
        .\venv\Scripts\activate
        ```

3.  **Instalar as dependências:**
    ```bash
    pip3 install -r scripts/requirements.txt
    ```

## 2. Execução do Pipeline

O pipeline é iniciado executando o `app_wildlife.py` com dois argumentos: o caminho para o arquivo de dados brutos (CSV) e o caminho para o arquivo JSON que descreve as colunas do CSV.

**Exemplo de uso:**
```bash
python scripts/Wildlife/app_wildlife.py rawdata/jaguar_mamiraua.csv rawdata/jaguar_columns.json
```

O pipeline executa as seguintes etapas principais:

### Etapa 1: Preparação dos Dados

-   **Responsável:** `Data_preparation/separar_localizacoes_individuais.py`
-   **Descrição:** O script lê o arquivo de dados brutos e o divide em arquivos CSV separados para cada animal, com base em seu ID. Esses arquivos individuais são salvos no diretório `scripts/Results/<nome_dataset>/`.
-   **Funções de Apoio:** Funções do `Common/utils.py` são usadas para ler os dados, obter a lista de animais e gerenciar os diretórios de resultados.

### Etapa 2: Interpolação de Dados (Preenchimento de Lacunas)

-   **Responsáveis:** `Interpolation/nbeat_trainer.py`, `Interpolation/nhits_trainer.py`, `Interpolation/nbeat_interpolation.py`, `Interpolation/nhits_interpolation.py`
-   **Descrição:** Esta etapa lida com a falta de dados nos rastreamentos de GPS.
    1.  **Treinamento (Opcional/Separado):** Os modelos de deep learning **N-BEATS** e **N-HITS** são treinados com os dados existentes. As funções `nhits_main_training_list` e `train_nbeats_model_list` são usadas para isso. Esta etapa é computacionalmente intensiva e geralmente é executada separadamente.
    2.  **Interpolação:** Os modelos treinados são usados para preencher as lacunas nos dados de localização de cada animal. Os resultados são novos arquivos CSV com os dados interpolados.
    3.  **Merge:** Os dados interpolados são consolidados com os dados originais.

### Etapa 3: Avaliação da Interpolação

-   **Responsáveis:** `Evaluation/average_by_individual.py`, `Evaluation/media_tempos_hist.py`, `Evaluation/average_comparison.py`
-   **Descrição:** Métricas de avaliação são calculadas para comparar a qualidade dos dados interpolados com os dados originais e entre os diferentes métodos de interpolação. São gerados gráficos e relatórios para análise.

### Etapa 4: Clusterização de Localizações

-   **Responsáveis:** Scripts no diretório `Clusterization/` (e.g., `kmeans_individual_csv.py`, `SOM_individual.py`, `BIRCH.py`).
-   **Descrição:** Os dados de localização (brutos e interpolados) são processados por algoritmos de clusterização para identificar áreas de interesse ou centros de atividade dos animais.
-   **Algoritmos Utilizados:** K-Means, SOM (Mapas Auto-Organizáveis), BIRCH e Mean Shift.
-   **Visualização:** Scripts como `plot_kmeans_som_birch_mean_shift.py` e `plot_dispersion_geral.py` são usados para gerar visualizações gráficas dos clusters e da dispersão dos pontos.

### Etapa 5: Geração de Contatos para DTN

-   **Responsáveis:** Scripts no diretório `DTN/`.
-   **Descrição:** Esta é a etapa central para a simulação de DTN. Ela calcula as oportunidades de contato entre os "nós" da rede (que podem ser os animais, os centroides dos clusters ou pontos fixos).
    1.  **Contatos Animal-Animal:** `DTN/find_contacts_between_nodes.py` calcula quando dois animais estão próximos o suficiente para uma comunicação.
    2.  **Contatos Animal-Centroide:** `DTN/find_contacs_animal_to_centroids.py` calcula os contatos entre os animais e os centroides (pontos de acesso virtuais) definidos na etapa de clusterização.
    3.  **Contatos com Pontos Fixos:** `DTN/add_uakari_lodge_contact.py` adiciona contatos com locais específicos, como o Uakari Lodge.
    4.  **Banco de Dados:** Um banco de dados (provavelmente SQLite ou similar, gerenciado por `DTN/setup_database.py`) é usado para armazenar e gerenciar os contatos gerados.
    5.  **Exportação da Traçabilidade:** `DTN/export_all_final_contacts.py` consolida todos os eventos de contato (início, fim, IDs dos nós) em um formato de arquivo de texto. Este arquivo é a "traçabilidade de mobilidade", que serve de entrada para o simulador DTN.

## 3. Simulação no The ONE

-   **Diretório:** `the_one/`
-   **Descrição:** O diretório `the_one` contém o simulador DTN **The Opportunistic Network Environment (The ONE)**.
-   **Entrada:** Os arquivos de traço de contato gerados na Etapa 5 são usados como entrada para as simulações no The ONE.
-   **Configuração:** Arquivos como `default_settings.txt` e `jaguar_mamiraua_generic_settings.txt` são usados para configurar os parâmetros da simulação, como algoritmos de roteamento, tamanho dos buffers, etc.
-   **Execução:** A simulação é executada através dos scripts `one.bat` (Windows) ou `one.sh` (Linux), que processam o arquivo de configuração e a traçabilidade de contatos.
-   **Saída:** O simulador gera relatórios detalhados sobre a entrega de mensagens, latência, overhead e outras métricas de desempenho da rede DTN.

## 4. Tutorial: Pré-configuração das Simulações

Este tutorial mostra os passos para configurar o pipeline de geração de dados e o simulador The ONE para executar diferentes cenários de simulação.

### 4.1. Configurando o Pipeline de Geração de Dados (Python)

O principal arquivo de configuração para o pipeline é o `scripts/Wildlife/app_wildlife.py`. A maior parte da configuração é feita comentando ou descomentando seções de código para controlar quais etapas são executadas.

**A. Fase de Treinamento dos Modelos de Interpolação:**
- **Objetivo:** Treinar os modelos N-BEATS e N-HITS. Esta etapa é intensiva e só precisa ser feita uma vez por dataset ou se os parâmetros do modelo mudarem.
- **Como fazer:**
  - Abra `scripts/Wildlife/app_wildlife.py`.
  - Localize a seção `############## INTERPOLATION ##############################`.
  - Descomente as linhas que chamam as funções de treinamento:
    ```python
    # nhits_main_training_list(list_animals, file_rawdata, file_rawdata_columns)
    # sys.exit()

    # train_nbeats_model_list(list_animals, file_rawdata, file_rawdata_columns)
    # sys.exit()
    ```
  - Execute o `app_wildlife.py`. Após o término, comente essas linhas novamente.

**B. Fase de Geração de Contatos:**
- **Objetivo:** Controlar a geração dos arquivos finais de contato que serão usados pelo The ONE.
- **Como fazer:**
  - No final do `app_wildlife.py`, você encontrará as listas que definem os parâmetros para os experimentos.
    ```python
    centroids_list = [8, 16, 32]
    algorithms_list = ["kmeans", "birch", "som"]
    interpolations_list = ["rawdata"]
    ```
  - **`centroids_list`**: Defina o número de centroides que você deseja testar (e.g., 8, 16, 32).
  - **`algorithms_list`**: Escolha os algoritmos de clusterização cujos resultados você quer usar para gerar os contatos (e.g., "kmeans", "birch", "som").
  - **`interpolations_list`**: Defina se quer usar os dados brutos (`rawdata`) ou os dados interpolados (`nbeat`, `nhits`) como base para os contatos.
  - O script irá iterar sobre todas as combinações e gerar um arquivo de contato para cada uma, usando a função `run_export_all_final_trace`.

### 4.2. Configurando o Simulador The ONE

A configuração do The ONE é feita através de arquivos de texto no diretório `the_one/`. O arquivo principal é o `default_settings.txt`.

**A. Apontar para o Arquivo de Contato Correto:**
- **Objetivo:** Dizer ao The ONE qual cenário de contato ele deve simular.
- **Como fazer:**
  - Abra o arquivo `the_one/default_settings.txt`.
  - Encontre a linha `Events1.filePath`.
  - Altere o valor para o caminho do arquivo de contato que você gerou na etapa anterior. O arquivo estará em `the_one/data/`.
    ```ini
    # Exemplo: usar contatos gerados com 32 centroides pelo algoritmo BIRCH
    Events1.filePath = data/jaguar_mamiraua_contacts_32_centroids_birch_rawdata.txt
    ```

**B. Definir o Protocolo de Roteamento:**
- **Objetivo:** Escolher qual protocolo DTN será testado.
- **Como fazer:**
  - Em `default_settings.txt`, localize a linha `Group.router`.
  - Altere o valor para o protocolo desejado (e.g., `MaxPropRouter`, `ProphetRouter`, `SprayAndWaitRouter`).
    ```ini
    # Exemplo: usar o protocolo Prophet
    Group.router = ProphetRouter
    ```

**C. Configurar a Geração de Mensagens:**
- **Objetivo:** Definir como as mensagens são criadas na simulação.
- **Como fazer:**
  - Na seção `Events2`, você pode configurar o `interval` (tempo entre a criação de mensagens), `size` (tamanho das mensagens), `hosts` (nós que criam as mensagens) e `toHosts` (nós de destino).
    ```ini
    # Exemplo: criar mensagens de 256 bytes a cada 10 minutos (600s)
    Events2.interval = 600
    Events2.size = 256
    ```

**D. Definir o Diretório de Resultados:**
- **Objetivo:** Organizar os resultados da simulação.
- **Como fazer:**
  - É uma boa prática nomear o diretório de relatórios de acordo com o cenário que está sendo testado.
  - Em `default_settings.txt`, altere a linha `Report.reportDir`.
    ```ini
    # Exemplo: salvar relatórios em um diretório específico para o cenário
    Report.reportDir = reports/cenario_prophet_32_birch
    ```

Após configurar o pipeline e o `default_settings.txt`, você pode executar a simulação usando `one.bat` ou `one.sh` e os resultados serão salvos no diretório de relatório especificado.
