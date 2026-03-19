# Tutorial: Geração de Contatos DTN no Projeto Wildlife

Este documento detalha o funcionamento da seção de código responsável pela geração de contatos DTN (Delay-Tolerant Networking) no arquivo `app_wildlife.py`. Essa parte do script é o núcleo da simulação de contatos entre os animais, pontos de interesse (centroids) e locais fixos (Uakari Lodge).

## Visão Geral do Processo

O script executa uma sequência de passos para simular e registrar oportunidades de contato. O fluxo principal é:

1.  **Preparação**: Combinações de animais são criadas e o banco de dados de contatos é limpo.
2.  **Contatos entre Animais**: Itera sobre todos os pares de animais para encontrar e registrar contatos diretos entre eles.
3.  **Exportação Inicial**: Consolida os contatos entre animais em um arquivo de rastreamento.
4.  **Contatos com Pontos Fixos**: Calcula os contatos de cada animal com "centroids" (clusters de localização) e um ponto de interesse fixo (Uakari Lodge).
5.  **Loop de Experimentos**: Gera múltiplos arquivos de saída, variando parâmetros como o número de centroids e o algoritmo de clusterização, para análise comparativa.

A seguir, cada passo é explicado em detalhe.

---

## Análise Detalhada do Código

### 1. Preparação

```python
#Combinação sem repetições
pairs = create_combinations(list_animals)
```

-   **Função:** `create_combinations(list_animals)`
-   **Origem:** `Common.utils`
-   **Objetivo:** Esta função recebe a lista de todos os animais (`list_animals`) e cria uma lista de todos os pares possíveis de animais, sem repetições. Por exemplo, se a lista for `['93', '94', '95']`, ela gerará `[('93', '94'), ('93', '95'), ('94', '95')]`. Isso é essencial para analisar os contatos entre cada par de onças.

```python
# Chamar todos os scripts de criação de dados de distancias e plots
import subprocess
subprocess.run([r"venv\Scripts\python.exe", r"scripts\DTN\generate_all_distances_data_n_plots.py"])
```

-   **Comando:** `subprocess.run(...)`
-   **Objetivo:** Este comando executa um script Python externo: `generate_all_distances_data_n_plots.py`.
-   **Funcionalidade:** Pelo nome, este script provavelmente calcula as distâncias entre os animais ao longo do tempo e gera gráficos (plots) para visualização e análise dessas distâncias. É um passo preparatório para a detecção de contatos. Além disso, é a partir dos resultados de tempo de residência que encontraremos o tempo de contato entre animais e centroids, baseado na distância entre o animal e o centroid.


```python
# Limpar o database para gerar novamente os contatos
run_recreate_table()
```

-   **Função:** `run_recreate_table()`
-   **Origem:** Importada como `recreate_table` do módulo `DTN.setup_database`.
-   **Objetivo:** Esta função apaga e recria a tabela no banco de dados onde os contatos são armazenados. Isso garante que cada execução do script comece com dados "limpos", evitando duplicatas ou resultados de simulações anteriores.

### 2. Geração de Contatos entre Pares de Animais

```python
for pair in pairs:
    run_find_contacts_between_nodes(pair[0], pair[1], file_rawdata)
    run_add_down_event(f'{pair[0]}_{pair[1]}', file_rawdata)
```

-   **Loop:** O código itera sobre cada par de animais (`pair`) gerado anteriormente.
-   **Função:** `run_find_contacts_between_nodes(pair[0], pair[1], file_rawdata)`
    -   **Origem:** Importada como `run` do módulo `DTN.find_contacts_between_nodes`.
    -   **Objetivo:** Para cada par de animais (`pair[0]` e `pair[1]`), esta função analisa seus dados de localização (`file_rawdata`) para identificar e registrar no banco de dados os momentos em que eles estiveram próximos o suficiente para estabelecer um "contato" (baseado em um raio de proximidade definido).
-   **Função:** `run_add_down_event(f'{pair[0]}_{pair[1]}', file_rawdata)`
    -   **Origem:** Importada como `run` do módulo `DTN.add_down_event`.
    -   **Objetivo:** Após um contato, um evento "down" (conexão encerrada) precisa ser registrado. Esta função provavelmente adiciona esse evento ao final de cada janela de contato para o par de animais especificado, significando que a comunicação foi encerrada.

### 3. Consolidação dos Contatos

```python
# Fora do loop dos pares de animais
print("Gerando arquivo final consolidado...")
run_export_final_trace(file_rawdata)
```

-   **Função:** `run_export_final_trace(file_rawdata)`
-   **Origem:** Importada como `run` do módulo `DTN.export_final_contacts`.
-   **Objetivo:** Após processar todos os pares de animais, esta função consulta o banco de dados, coleta todos os registros de contato (eventos "up" e "down") e os exporta para um único arquivo de rastreamento consolidado. Este arquivo pode ser usado como entrada para simuladores de rede DTN como o The ONE.

### 4. Geração de Contatos com Centroids e Uakari Lodge

```python
list_animals = ['93', '94', '95', '96', '97', '98', '99', '100']

# Criar contatos entre onças e centroids
for animal_id in list_animals:
    run_contacts_animal_centroids(animal_id, file_rawdata)

# Criar contatos entre onças e o Uakari Lodge
for animal_id in list_animals:
    run_contacts_animal_uakari(animal_id, file_rawdata)
```
- **Nota**: A `list_animals` é redefinida aqui. Isso pode ser intencional para focar em um subconjunto específico de animais para esta parte da análise.

-   **Função:** `run_contacts_animal_centroids(animal_id, file_rawdata)`
    -   **Origem:** Importada como `run` do módulo `DTN.find_contacs_animal_to_centroids`.
    -   **Objetivo:** Esta função calcula os contatos entre cada onça (`animal_id`) e os "centroids". Centroids são pontos centrais de clusters de localizações (calculados em outra parte do código), que podem representar áreas de interesse ou pontos de encontro. Isso simula a entrega de dados a "data mules" ou pontos de coleta de dados estáticos.

-   **Função:** `run_contacts_animal_uakari(animal_id, file_rawdata)`
    -   **Origem:** Importada como `run` do módulo `DTN.add_uakari_lodge_contact`.
    -   **Objetivo:** Semelhante à função anterior, mas especificamente para calcular e registrar contatos entre cada animal e a localização do Uakari Lodge, que atua como um ponto de coleta de dados central e fixo na simulação.

### 5. Geração de Arquivos de Experimentos

```python
raw_name = "jaguar_mamiraua"
    
# Listas para o loop de experimentos
centroids_list = [8, 16, 32]
algorithms_list = ["kmeans", "birch", "som"]
interpolations_list = ["rawdata"] 

# Gerar arquivos para cada combinação
for n in centroids_list:
    for alg in algorithms_list:
        for interp in interpolations_list:
            run_export_all_final_trace(raw_name, n_centroids=n, algorithm=alg, interpolation=interp)
```

-   **Objetivo:** Esta seção final automatiza a geração de múltiplos arquivos de rastreamento para diferentes cenários experimentais.
-   **Loop:** O código itera sobre três listas:
    -   `centroids_list`: O número de clusters (8, 16, 32).
    -   `algorithms_list`: Os algoritmos de clusterização usados para definir os centroids (`kmeans`, `birch`, `som`).
    -   `interpolations_list`: O tipo de dado usado (neste caso, apenas `rawdata`, mas poderia incluir dados interpolados como `nbeats` ou `nhits`).
-   **Função:** `run_export_all_final_trace(raw_name, n_centroids=n, algorithm=alg, interpolation=interp)`
    -   **Origem:** Importada como `run` do módulo `DTN.export_all_final_contacts`.
    -   **Objetivo:** Para cada combinação de parâmetros (ex: 32 centroids, usando o algoritmo `birch` com `rawdata`), esta função gera um arquivo de contato final específico. Ela consulta o banco de dados filtrando pelos contatos relevantes para aquela configuração e exporta o resultado. Isso permite comparar o impacto de diferentes configurações de clusterização e número de pontos de coleta na conectividade da rede.
