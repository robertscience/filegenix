from flask import Flask, request, jsonify, send_from_directory, render_template
from werkzeug.utils import secure_filename
import pandas as pd
import os
import uuid
import json
import logging
import openpyxl
import zipfile
import nbformat

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = os.getenv('UPLOAD_DIR', 'Uploads')  # Fallback local: 'Uploads', prod: /opt/render/project/src/Uploads
app.config['MAX_CONTENT_LENGTH'] = 512 * 1024 * 1024  # 512 MB
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Configurar logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def validate_xlsx(file_path, filename):
    try:
        workbook = openpyxl.load_workbook(file_path, read_only=True)
        sheet_names = workbook.sheetnames
        logger.debug(f"Hojas en {filename}: {sheet_names}")
        if not sheet_names:
            return False, "No sheets found in the Excel file"
        return True, sheet_names
    except zipfile.BadZipFile:
        return False, "Corrupted or invalid Excel file"
    except Exception as e:
        return False, f"Validation error: {str(e)}"

@app.route('/')
def index():
    logger.debug("Accediendo a la ruta raíz")
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    logger.debug("Iniciando subida de archivos")
    if 'files' not in request.files:
        logger.error("No se encontraron archivos en la solicitud")
        return jsonify({'error': 'No files uploaded'}), 400

    files = request.files.getlist('files')
    logger.debug(f"Archivos recibidos: {[file.filename for file in files]}")
    if not files or all(not allowed_file(file.filename) for file in files):
        logger.error("No se encontraron archivos válidos")
        return jsonify({'error': 'No valid files uploaded'}), 400

    upload_id = str(uuid.uuid4())
    upload_path = os.path.join(app.config['UPLOAD_FOLDER'], upload_id)
    os.makedirs(upload_path, exist_ok=True)
    logger.debug(f"Carpeta creada: {upload_path}")

    results = {}
    seen_filenames = set()
    for file in files:
        if file and allowed_file(file.filename):
            original_filename = file.filename
            filename = original_filename  # Mantenemos el nombre original en todo el proceso
            if filename in seen_filenames:
                logger.warning(f"Archivo duplicado ignorado: {original_filename}")
                results[original_filename] = {'error': 'Duplicate file name'}
                continue
            seen_filenames.add(filename)
            file_path = os.path.join(upload_path, filename)
            logger.debug(f"Original: {original_filename}, Guardado como: {filename}, Ruta: {file_path}")
            file.save(file_path)
            try:
                if filename.lower().endswith('.csv'):
                    df = pd.read_csv(file_path, encoding='utf-8')
                elif filename.lower().endswith(('.xlsx', '.xls')):
                    is_valid, sheet_info = validate_xlsx(file_path, original_filename)
                    if not is_valid:
                        logger.error(f"Validación fallida para {original_filename}: {sheet_info}")
                        results[original_filename] = {'error': sheet_info}
                        continue
                    engines = ['openpyxl', 'xlrd']
                    df = None
                    for engine in engines:
                        try:
                            df = pd.read_excel(file_path, engine=engine)
                            logger.debug(f"Éxito con engine {engine} para {original_filename}")
                            break
                        except Exception as e:
                            logger.error(f"Error con engine {engine} en {original_filename}: {str(e)}")
                    if df is None:
                        for sheet_name in sheet_info:
                            try:
                                df = pd.read_excel(file_path, engine='openpyxl', sheet_name=sheet_name)
                                logger.debug(f"Éxito con hoja {sheet_name} para {original_filename}")
                                break
                            except Exception as e:
                                logger.error(f"Error con hoja {sheet_name} en {original_filename}: {str(e)}")
                        if df is None:
                            raise Exception("All engines and sheets failed to read the Excel file")
                # Consolidar internamente
                consolidated_data = pd.concat([results.get('consolidated', pd.DataFrame()), df], axis=1) if 'consolidated' in results else df
                results['consolidated'] = consolidated_data
                results[original_filename] = {
                    'columns': df.columns.tolist(),
                    'shape': df.shape,
                    'nulls': df.isnull().sum().to_dict(),
                    'file_url': f'/files/{upload_id}/{original_filename}'  # Mantenemos el nombre original
                }
                logger.debug(f"Procesado {original_filename}: {df.shape}, Columnas: {df.columns.tolist()}")
            except Exception as e:
                logger.error(f"Error al procesar {original_filename}: {str(e)}")
                results[original_filename] = {'error': f'Failed to process: {str(e)}'}

    # Generar archivo consolidado y notebook
    if 'consolidated' in results:
        consolidated_path = os.path.join(upload_path, 'data_consolidated.xlsx')
        results['consolidated'].to_excel(consolidated_path, index=False)
        results['consolidated_file'] = {
            'file_url': f'/files/{upload_id}/data_consolidated.xlsx'
        }
        del results['consolidated']  # No exponer el DataFrame

        # Generar notebook con nombre dinámico (por ahora Entregable1_Proyecto.ipynb como ejemplo)
        notebook_name = 'Entregable1_Proyecto.ipynb'  # Cambiará según tu instrucción
        nb = nbformat.v4.new_notebook()
        nb.cells = [
            nbformat.v4.new_markdown_cell("# Entregable 1: Análisis y Consolidación de Datos para Optimización Estratégica\n## Preparado por RobertScience\n\n**Correo**: robertscience.ia@gmail.com | **Web**: robertScience.com\n\n**Objetivo**: Este entregable presenta un análisis exhaustivo basado en los datos proporcionados, consolidando información en un conjunto unificado para habilitar decisiones estratégicas de alto impacto. Los resultados reflejan un enfoque profesional que transforma los datos en valor accionable.\n\n**Entregables**:\n- `data_consolidated.xlsx`: Conjunto de datos consolidado con métricas derivadas.\n- Este documento: Detalle de los resultados y recomendaciones.\n- Visualizaciones: Gráficos de barras y mapas de calor.\n\n**RobertScience** | Soluciones Avanzadas en Data"),
            nbformat.v4.new_markdown_cell("## 1. Carga de Datos\nSe trabajaron los archivos: FACT_SALES, DIM_PRODUCT, DIM_CATEGORY, DIM_SEGMENT, y DIM_CALENDAR, integrándolos en un análisis consolidado.\n\n**Archivos**:\n- FACT_SALES: Tabla de hechos de ventas (122002 filas, 6 columnas).\n- DIM_PRODUCT: Dimensiones de productos (505 filas, 9 columnas).\n- DIM_CATEGORY: Dimensiones de categorías (5 filas, 2 columnas).\n- DIM_SEGMENT: Dimensiones de segmentos (53 filas, 6 columnas).\n- DIM_CALENDAR: Dimensiones de calendario (156 filas, 5 columnas)."),
            nbformat.v4.new_code_cell("import pandas as pd\nimport matplotlib.pyplot as plt\nimport seaborn as sns\n\ndf_consolidated = pd.read_excel('data_consolidated.xlsx')\n\nprint('Estructura del conjunto consolidado:', df_consolidated.shape)\nprint('Columnas disponibles:', df_consolidated.columns.tolist())"),
            nbformat.v4.new_markdown_cell("## 2. Exploración Inicial de los Datos\nSe revisaron los datos para entender su estructura y calidad, preparando el terreno para un análisis estratégico."),
            nbformat.v4.new_code_cell("print('\\nPrimeras filas del conjunto consolidado:')\nprint(df_consolidated.head())\nprint('\\nInformación del conjunto consolidado:')\ndf_consolidated.info()\nprint('\\nValores nulos:', df_consolidated.isnull().sum().to_dict())"),
            nbformat.v4.new_markdown_cell("## 3. Limpieza de Datos\nSe optimizaron los datos para asegurar su calidad, eliminando inconsistencias y preparando métricas accionables."),
            nbformat.v4.new_code_cell("df_consolidated = df_consolidated.dropna(subset=['ITEM_CODE'])\ndf_consolidated = df_consolidated.drop_duplicates()\ndf_consolidated['ITEM_CODE'] = df_consolidated['ITEM_CODE'].str.strip().str.upper()\n\nprint('Estructura tras limpieza:', df_consolidated.shape)"),
            nbformat.v4.new_markdown_cell("## 4. Análisis Estratégico\nSe derivaron métricas clave y se generaron visualizaciones para apoyar la toma de decisiones."),
            nbformat.v4.new_code_cell("top_categories = df_consolidated.groupby('CATEGORY')['REVENUE'].sum().nlargest(5).reset_index()\n\nplt.figure(figsize=(12, 6))\nsns.barplot(data=top_categories, x='REVENUE', y='CATEGORY', palette='viridis', edgecolor='black')\nplt.title('Top 5 Categorías por Ingresos', pad=15)\nplt.xlabel('Ingresos')\plt.ylabel('Categorías')\nplt.grid(True, linestyle='--', alpha=0.7)\nfor i, v in enumerate(top_categories['REVENUE']):\n    plt.text(v + 1000, i, f'${v:,.0f}', va='center')\nplt.show()\n\nplt.figure(figsize=(10, 8))\ncorrelation_matrix = df_consolidated[['REVENUE', 'TOTAL_UNIT_SALES', 'REVENUE_PER_UNIT']].corr()\nsns.heatmap(correlation_matrix, annot=True, cmap='YlOrRd', fmt='.2f', square=True, linewidths=0.5)\plt.title('Correlaciones entre Métricas Clave', pad=15)\nplt.show()"),
            nbformat.v4.new_markdown_cell("## 5. Recomendaciones Estratégicas\nBasados en el análisis, se proponen estrategias para maximizar el valor del negocio:\n- Enfocarse en las categorías de mayor ingreso para optimizar recursos.\n- Aprovechar las correlaciones identificadas para ajustar estrategias de ventas.\n\n**Contacto**: Para más detalles, contáctenos en robertscience.ia@gmail.com. RobertScience: Su aliado en soluciones de datos.")
        ]
        notebook_path = os.path.join(upload_path, notebook_name)
        with open(notebook_path, 'w', encoding='utf-8') as f:
            nbformat.write(nb, f)
        results['notebook'] = {
            'file_url': f'/files/{upload_id}/{notebook_name}'
        }

    summary_path = os.path.join(upload_path, 'data_procesada.json')  # Nombre interno para mi uso
    with open(summary_path, 'w') as f:
        json.dump({'upload_id': upload_id, 'files': {k: v for k, v in results.items() if k not in ['consolidated']}}, f)
    logger.debug(f"Resumen guardado: {summary_path}")

    base_url = request.url_root.rstrip('/')
    summary_url = f"{base_url}/files/{upload_id}/data_procesada.json"  # Enlace interno para mí

    return jsonify({
        'upload_id': upload_id,
        'files': {k: v for k, v in results.items() if k not in ['consolidated']},
        'summary_url': summary_url  # Solo para mi uso interno
    })

@app.route('/files/<upload_id>/<filename>', methods=['GET'])
def get_file(upload_id, filename):
    logger.debug(f"Accediendo a archivo: {upload_id}/{filename}")
    try:
        return send_from_directory(os.path.join(app.config['UPLOAD_FOLDER'], upload_id), filename)
    except Exception as e:
        logger.error(f"Error al obtener archivo {filename}: {str(e)}")
        return jsonify({'error': f'File not found: {str(e)}'}), 404

@app.route('/files/<upload_id>', methods=['GET'])
def get_upload_summary(upload_id):
    summary_path = os.path.join(app.config['UPLOAD_FOLDER'], upload_id, 'data_procesada.json')
    logger.debug(f"Accediendo a resumen: {summary_path}")
    if not os.path.exists(summary_path):
        logger.error("Resumen no encontrado")
        return jsonify({'error': 'Upload ID not found'}), 404

    with open(summary_path, 'r') as f:
        summary = json.load(f)
    return jsonify(summary)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)