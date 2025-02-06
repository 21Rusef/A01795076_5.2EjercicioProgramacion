"""
Programa para calcular el costo total de ventas con base en productos.
Utiliza archivos JSON como entrada y genera un informe con los resultados.

Uso:
    python computeSales.py priceCatalogue.json salesRecord.json

Referencias:
    Docstring para funciones.
    https://www.datacamp.com/es/tutorial/docstrings-python
"""

import sys
import time
import json
import pandas as pd


def load_json_to_dataframe(file_path):
    """
    Carga un archivo JSON en un DataFrame de Pandas.

    :param file_path: Ruta del archivo JSON.
    :return: DataFrame con los datos cargados o vacío en caso de error.
    """
    try:
        return pd.read_json(file_path)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as error:
        print(f"Error al cargar el archivo {file_path}: {error}")
        return pd.DataFrame()


def map_product_prices(df_products):
    """
    Crea un diccionario con los precios de los productos para acceso rápido.

    :param df_products: DataFrame con información del catálogo de productos.
    :return: Diccionario con {nombre_producto: precio}.
    """
    return df_products.set_index("title")["price"].to_dict()


def process_sales_data(df_sales, product_prices):
    """
    Calcula costo total añadiendo el precio unitario.

    :param df_sales: DataFrame con las ventas.
    :param product_prices: Diccionario con los precios de los productos.
    :return: DataFrame procesado, DataFrame con errores.
    """
    df_sales["Unit_Price"] = df_sales["Product"].map(product_prices)
    df_sales["Unit_Price"].fillna("Producto no encontrado", inplace=True)

    df_sales = df_sales.assign(
        Total_Cost=df_sales.apply(
            lambda row: row["Quantity"] * row["Unit_Price"]
            if isinstance(row["Unit_Price"], (int, float))
            else None,
            axis=1
        )
    )

    errors = df_sales[df_sales["Total_Cost"].isna()]
    df_sales.dropna(subset=["Total_Cost"], inplace=True)

    return df_sales, errors


def generate_sales_report(df_sales, elapsed_time):
    """
    Genera un reporte con el total de ventas y el tiempo de ejecución.

    :param df_sales: DataFrame con las ventas procesadas.
    :param elapsed_time: Tiempo de ejecución del proceso.
    :return: Cadena con el reporte formateado.
    """
    total_sales_cost = df_sales["Total_Cost"].sum()

    return f"""
                Resultados de Ventas:
                ---------------------
                Total de Costos de Ventas: ${total_sales_cost:.2f}
                Tiempo de ejecución: {elapsed_time:.2f} segundos
                """


def save_report_to_file(report, file_path):
    """
    Guarda el reporte de ventas en un archivo txt.

    :param report: Reporte en formato de texto.
    :param file_path: Ruta del archivo de salida.
    """
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            file.write(report)
    except (FileNotFoundError, IOError) as error:
        print(f"Error al guardar el archivo {file_path}: {error}")


def main():
    """
    Función principal que ejecuta el flujo de procesamiento de ventas.
    - Valida que se pasen dos archivos JSON como argumentos.
    - Carga los archivos en DataFrames.
    - Calcula costos y genera el informe.
    - Guarda los resultados en un archivo de texto.
    """
    start_time = time.time()

    if len(sys.argv) != 3:
        print("\nArgumentos faltantes o inválidos")
        sys.exit(1)

    product_list_path, sales_record_path = sys.argv[1], sys.argv[2]

    df_products = load_json_to_dataframe(product_list_path)
    df_sales = load_json_to_dataframe(sales_record_path)

    product_prices = map_product_prices(df_products)
    df_sales, errors = process_sales_data(df_sales, product_prices)

    elapsed_time = time.time() - start_time
    report = generate_sales_report(df_sales, elapsed_time)

    output_file_path = "SalesResults.txt"
    save_report_to_file(report, output_file_path)

    print(report)
    print("\n", df_sales.head(10))

    if not errors.empty:
        print("\nErrores detectados:\n", errors.head(5))

    return output_file_path


if __name__ == "__main__":
    main()
