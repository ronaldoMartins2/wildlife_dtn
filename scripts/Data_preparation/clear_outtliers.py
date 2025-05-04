import pandas as pd

#def run(current_animal, file_rawdata):


def run(df, dataset_name="", exclude_cols=None):
    if exclude_cols is None:
        exclude_cols = []

    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    numeric_cols = [col for col in numeric_cols if col not in exclude_cols]

    print(f"\n[{dataset_name}] Colunas numéricas consideradas para remoção de outliers: {numeric_cols}")

    for col in numeric_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        total = df.shape[0]
        remaining = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)].shape[0]
        print(f"{col}: {total - remaining} outliers removidos, {remaining} linhas restantes")

    # Remove os outliers
    for col in numeric_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]

    print(f"[{dataset_name}] Linhas restantes após remoção de outliers: {df.shape[0]}")
    return df

def run_mock( ):
    # Carrega os dados

    df_tangara = pd.read_csv('./tangara_mata_atlantica.csv')
    df_jaguar = pd.read_csv('./jaguar_mamiraua.csv')

    # Processa com exclusão da coluna problemáica
    df_tangara_clean = run(df_tangara, dataset_name="Tangará", exclude_cols=["manually-marked-outlier"])
    df_jaguar_clean = run(df_jaguar, dataset_name="Jaguar")

    # Salva os arquivos finais
    tangara_clean_path = './tangara_mata_atlantica_clean.csv'
    jaguar_clean_path = './jaguar_mamiraua_clean.csv'

    df_tangara_clean.to_csv(tangara_clean_path, index=False)
    df_jaguar_clean.to_csv(jaguar_clean_path, index=False)

    print("\n✅ Arquivos processados e salvos:")
    print(f"- {tangara_clean_path}")
    print(f"- {jaguar_clean_path}")
