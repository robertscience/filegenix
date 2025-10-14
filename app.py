from flask import Flask, request, jsonify, render_template
from werkzeug.utils import secure_filename
import pandas as pd
import os
import uuid
import json
import logging
import openpyxl
import zipfile

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
            if original_filename in seen_filenames:
                logger.warning(f"Archivo duplicado ignorado: {original_filename}")
                results[original_filename] = {'error': 'Duplicate file name'}
                continue
            seen_filenames.add(original_filename)
            file_path = os.path.join(upload_path, original_filename)  # Usar nombre original
            logger.debug(f"Original: {original_filename}, Guardado como: {original_filename}, Ruta: {file_path}")
            file.save(file_path)
            try:
                if original_filename.lower().endswith('.csv'):
                    df = pd.read_csv(file_path, encoding='utf-8')
                elif original_filename.lower().endswith(('.xlsx', '.xls')):
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
                    'file_url': f'/files/{upload_id}/{original_filename}'  # Usar nombre original
                }
                logger.debug(f"Procesado {original_filename}: {df.shape}, Columnas: {df.columns.tolist()}")
            except Exception as e:
                logger.error(f"Error al procesar {original_filename}: {str(e)}")
                results[original_filename] = {'error': f'Failed to process: {str(e)}'}

    base_url = request.url_root.rstrip('/')
    summary_url = f"{base_url}/files/{upload_id}"

    return jsonify({
        'upload_id': upload_id,
        'files': results,
        'summary_url': summary_url
    })

@app.route('/files/<upload_id>', methods=['GET'])
def get_upload_summary(upload_id):
    upload_path = os.path.join(app.config['UPLOAD_FOLDER'], upload_id)
    logger.debug(f"Accediendo a resumen para upload_id: {upload_id}")
    if not os.path.exists(upload_path):
        logger.error("Upload ID no encontrado")
        return jsonify({'error': 'Upload ID not found'}), 404

    # Recolectar metadatos de los archivos subidos directamente
    files = os.listdir(upload_path)
    results = {}
    for filename in files:
        file_path = os.path.join(upload_path, filename)
        if allowed_file(filename):
            try:
                if filename.lower().endswith('.csv'):
                    df = pd.read_csv(file_path, encoding='utf-8')
                elif filename.lower().endswith(('.xlsx', '.xls')):
                    is_valid, _ = validate_xlsx(file_path, filename)
                    if not is_valid:
                        results[filename] = {'error': 'Invalid Excel file'}
                        continue
                    engines = ['openpyxl', 'xlrd']
                    df = None
                    for engine in engines:
                        try:
                            df = pd.read_excel(file_path, engine=engine)
                            break
                        except Exception:
                            continue
                    if df is None:
                        raise Exception("Failed to read Excel file")
                results[filename] = {
                    'columns': df.columns.tolist(),
                    'shape': df.shape,
                    'nulls': df.isnull().sum().to_dict(),
                    'file_url': f'/files/{upload_id}/{filename}'  # Usar nombre original
                }
            except Exception as e:
                results[filename] = {'error': f'Failed to process: {str(e)}'}

    return jsonify({
        'upload_id': upload_id,
        'files': results
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)