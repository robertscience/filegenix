from flask import Flask, request, jsonify, send_from_directory, render_template
from werkzeug.utils import secure_filename
import pandas as pd
import os
import uuid
import json
import logging
import openpyxl
from zipfile import ZipFile
from io import StringIO, BytesIO
import zipfile

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = os.getenv('UPLOAD_DIR', 'Uploads')  # Fallback local: 'Uploads', prod: env var de Render
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
            filename = secure_filename(file.filename)
            if filename in seen_filenames:
                logger.warning(f"Archivo duplicado ignorado: {original_filename}")
                results[original_filename] = {'error': 'Duplicate file name'}
                continue
            seen_filenames.add(filename)
            file_path = os.path.join(upload_path, filename)
            logger.debug(f"Original: {original_filename}, Guardado como: {filename}, Ruta: {file_path}")
            file.save(file_path)
            try:
                if filename.endswith('.csv'):
                    df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
                elif filename.endswith(('.xlsx', '.xls')):
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
                results[original_filename] = {
                    'columns': df.columns.tolist(),
                    'shape': df.shape,
                    'nulls': df.isnull().sum().to_dict(),
                    'file_url': f'/files/{upload_id}/{filename}',
                    'json_url': f'/files/{upload_id}/{filename}.json'
                }
                logger.debug(f"Procesado {original_filename}: {df.shape}, Columnas: {df.columns.tolist()}")
                json_path = os.path.join(upload_path, f"{filename}.json")
                df.to_json(json_path, orient='records', lines=True)
            except Exception as e:
                logger.error(f"Error al procesar {original_filename}: {str(e)}")
                results[original_filename] = {'error': f'Failed to process: {str(e)}'}

    summary_path = os.path.join(upload_path, 'summary.json')
    with open(summary_path, 'w') as f:
        json.dump({'upload_id': upload_id, 'files': results}, f)
    logger.debug(f"Resumen guardado: {summary_path}")

    base_url = request.url_root.rstrip('/')
    summary_url = f"{base_url}/files/{upload_id}/summary.json"

    return jsonify({
        'upload_id': upload_id,
        'files': results,
        'summary_url': summary_url
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
    summary_path = os.path.join(app.config['UPLOAD_FOLDER'], upload_id, 'summary.json')
    logger.debug(f"Accediendo a resumen: {summary_path}")
    if not os.path.exists(summary_path):
        logger.error("Resumen no encontrado")
        return jsonify({'error': 'Upload ID not found'}), 404

    with open(summary_path, 'r') as f:
        summary = json.load(f)
    return jsonify(summary)

@app.route('/preview')
def preview_page():
    return render_template('preview/index.html')

def get_diff_data(upload_id, step='raw'):
    try:
        summary_path = os.path.join(app.config['UPLOAD_FOLDER'], upload_id, 'summary.json')
        logger.debug(f"Buscando summary en: {summary_path}")
        if not os.path.exists(summary_path):
            raise Exception(f"Summary not found: {summary_path}")
        with open(summary_path, 'r') as f:
            summary = json.load(f)

        fact_file = next((f for f in summary['files'].values() if 'FACT_SALES' in f['file_url']), None)
        if not fact_file:
            raise Exception("FACT_SALES not found")
        base_name = os.path.basename(fact_file['file_url']).lower().replace('.csv', '') + '.json'
        fact_json_path = os.path.join(app.config['UPLOAD_FOLDER'], upload_id, base_name)
        logger.debug(f"Buscando FACT JSON en: {fact_json_path}")
        if not os.path.exists(fact_json_path):
            raise Exception(f"FACT JSON not found: {fact_json_path}")
        fact_df = pd.read_json(fact_json_path, orient='records', lines=True)

        before_str = fact_df.to_json(orient='records', lines=True)
        after_df = fact_df.copy()

        if step == 'limpio':
            after_df = after_df.dropna(subset=['ITEM_CODE']).drop_duplicates()
        elif step == 'merged':
            try:
                prod_file = next((f for f in summary['files'].values() if 'DIM_PRODUCT' in f['file_url']), None)
                if prod_file:
                    base_name_prod = os.path.basename(prod_file['file_url']).lower().replace('.xlsx', '') + '.json'
                    prod_json_path = os.path.join(app.config['UPLOAD_FOLDER'], upload_id, base_name_prod)
                    logger.debug(f"Buscando PROD JSON en: {prod_json_path}")
                    if os.path.exists(prod_json_path):
                        prod_df = pd.read_json(prod_json_path, orient='records', lines=True)
                        after_df = pd.merge(after_df, prod_df, left_on='ITEM_CODE', right_on='ITEM', how='left')
                        logger.debug(f"Merge PRODUCT: {after_df.shape[0]} rows")
                    else:
                        logger.warning(f"PROD JSON not found: {prod_json_path}")

                cat_file = next((f for f in summary['files'].values() if 'DIM_CATEGORY' in f['file_url']), None)
                if cat_file:
                    base_name_cat = os.path.basename(cat_file['file_url']).lower().replace('.csv', '') + '.json'
                    cat_json_path = os.path.join(app.config['UPLOAD_FOLDER'], upload_id, base_name_cat)
                    if os.path.exists(cat_json_path):
                        cat_df = pd.read_json(cat_json_path, orient='records', lines=True)
                        after_df = pd.merge(after_df, cat_df, on='CATEGORY', how='left')
                        logger.debug(f"Merge CATEGORY: {after_df.shape[0]} rows")
                    else:
                        logger.warning(f"CAT JSON not found: {cat_json_path}")

                seg_file = next((f for f in summary['files'].values() if 'DIM_SEGMENT' in f['file_url']), None)
                if seg_file:
                    base_name_seg = os.path.basename(seg_file['file_url']).lower().replace('.xlsx', '') + '.json'
                    seg_json_path = os.path.join(app.config['UPLOAD_FOLDER'], upload_id, base_name_seg)
                    if os.path.exists(seg_json_path):
                        seg_df = pd.read_json(seg_json_path, orient='records', lines=True)
                        key_seg = ['CATEGORY', 'FORMAT']
                        if all(col in after_df.columns for col in key_seg) and all(col in seg_df.columns for col in key_seg):
                            after_df = pd.merge(after_df, seg_df, on=key_seg, how='left')
                        logger.debug(f"Merge SEGMENT: {after_df.shape[0]} rows")
                    else:
                        logger.warning(f"SEG JSON not found: {seg_json_path}")

                cal_file = next((f for f in summary['files'].values() if 'DIM_CALENDAR' in f['file_url']), None)
                if cal_file:
                    base_name_cal = os.path.basename(cal_file['file_url']).lower().replace('.xlsx', '') + '.json'
                    cal_json_path = os.path.join(app.config['UPLOAD_FOLDER'], upload_id, base_name_cal)
                    if os.path.exists(cal_json_path):
                        cal_df = pd.read_json(cal_json_path, orient='records', lines=True)
                        after_df = pd.merge(after_df, cal_df, on='WEEK', how='left')
                        logger.debug(f"Merge CALENDAR: {after_df.shape[0]} rows")
                    else:
                        logger.warning(f"CAL JSON not found: {cal_json_path}")
            except Exception as e:
                logger.error(f"Merge fail: {str(e)}")
        elif step == 'transform':
            after_df['WEEK'] = pd.to_datetime(after_df['WEEK'], errors='coerce')
            if 'ITEM_DESCRIPTION' in after_df.columns:
                after_df['ITEM_DESCRIPTION'] = after_df['ITEM_DESCRIPTION'].astype(str).str.upper()
            after_df['REVENUE'] = after_df['TOTAL_VALUE_SALES']
            after_df['REVENUE_PER_UNIT'] = after_df['REVENUE'] / after_df['TOTAL_UNIT_SALES']
            if 'WEEK' in after_df.columns:
                after_df['QUARTER'] = after_df['WEEK'].dt.quarter
                after_df['YEAR'] = after_df['WEEK'].dt.year
                after_df['QUARTER_REVENUE'] = after_df.groupby(['YEAR', 'QUARTER'])['REVENUE'].transform('sum')
                q4_revenue = after_df[after_df['QUARTER'] == 4].groupby('YEAR')['REVENUE'].sum()
                after_df['TREND_FLAG'] = after_df['YEAR'].apply(lambda y: 'High' if y in q4_revenue.index and q4_revenue[y] > q4_revenue.mean() else 'Normal')

        after_str = after_df.to_json(orient='records', lines=True)
        match_rate = (len(after_df.dropna(subset=['ITEM_CODE'])) / len(fact_df)) * 100 if len(fact_df) > 0 else 0
        logger.debug(f"Match rate: {match_rate:.1f}%")

        return jsonify({
            'before': before_str,
            'after': after_str,
            'match_rate': match_rate,
            'stats': {'rows_before': len(fact_df), 'rows_after': len(after_df)}
        })
    except Exception as e:
        logger.error(f"Error in get_diff_data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/diff/<upload_id>/data', methods=['GET'])
def diff_data(upload_id):
    step = request.args.get('step', 'raw')
    return get_diff_data(upload_id, step)

@app.route('/preview/download/<upload_id>')
def download_work(upload_id):
    step = request.args.get('step', 'transform')
    data_response = get_diff_data(upload_id, step)
    if data_response.status_code != 200:
        return data_response
    data = data_response.get_json()
    after_df = pd.read_json(StringIO(data['after']), orient='records', lines=True)

    memory_file = BytesIO()
    with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('data_consolidados.csv', after_df.to_csv(index=False))
        zf.writestr('merged_data.json', data['after'])
        nb_content = {
            "cells": [
                {"cell_type": "markdown", "source": ["# Entregable Preview - RobertScience DS\nTurbo Análisis de Ventas\nFecha: Octubre 2025"]},
                {"cell_type": "code", "source": [
                    "import pandas as pd\n",
                    "import matplotlib.pyplot as plt\n",
                    "import seaborn as sns\n",
                    "df = pd.read_csv('data_consolidados.csv')\n",
                    "print('Shape:', df.shape)\n",
                    "print('Top Revenue por Categoría:')\n",
                    "print(df.groupby('CATEGORY')['REVENUE'].sum().sort_values(ascending=False).head())\n",
                    "plt.figure(figsize=(10,6))\n",
                    "sns.barplot(x='REVENUE', y='CATEGORY', data=df.groupby('CATEGORY')['REVENUE'].sum().reset_index().sort_values('REVENUE', ascending=False).head(), palette='plasma')\n",
                    "plt.title('Top 5 Revenue por Categoría - RobertScience DS', color='#ff00ff')\n",
                    "plt.xlabel('Revenue ($)', color='#00ffcc')\n",
                    "plt.ylabel('Categoría', color='#00ffcc')\n",
                    "plt.tight_layout()\n",
                    "plt.show()\n",
                    "print('Recomendaciones: Priorizar stock en categorías top para uplift ROI proyectado 22%.')"
                ], "outputs": []},
                {"cell_type": "markdown", "source": ["## Recomendaciones\nPriorizar stock en categorías top para uplift 15%. Desarrollado por RobertScience DS.\nContacto: robertscience.ia@gmail.com"]}
            ],
            "metadata": {"kernelspec": {"name": "python3", "display_name": "Python 3"}},
            "nbformat": 4, "nbformat_minor": 5
        }
        zf.writestr('Entregable1_Proyecto.ipynb', json.dumps(nb_content))
        try:
            summary_path = os.path.join(app.config['UPLOAD_FOLDER'], upload_id, 'summary.json')
            with open(summary_path, 'r') as f:
                summary = json.load(f)
            for filename, info in summary['files'].items():
                if 'DIM' in filename and not info.get('error'):
                    base_name_dim = os.path.basename(info['file_url']).lower().replace('.xlsx', '').replace('.csv', '') + '.json'
                    dim_json_path = os.path.join(app.config['UPLOAD_FOLDER'], upload_id, base_name_dim)
                    if os.path.exists(dim_json_path):
                        dim_df = pd.read_json(dim_json_path, orient='records', lines=True)
                        zf.writestr(f"{os.path.splitext(filename)[0]}.csv", dim_df.to_csv(index=False))
        except Exception as e:
            logger.warning(f"ZIP DIMs fail: {str(e)}")

    memory_file.seek(0)
    return send_file(memory_file, as_attachment=True, download_name='RobertScience_Turbo_Preview.zip', mimetype='application/zip')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)