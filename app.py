from flask import Flask, request, jsonify, send_from_directory, render_template
from werkzeug.utils import secure_filename
import pandas as pd
import os
import uuid
import json
import logging

app = Flask(__name__)
UPLOAD_FOLDER = 'Uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Configurar logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

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
    for file in files:
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(upload_path, filename)
            logger.debug(f"Guardando archivo: {filename}")
            file.save(file_path)
            try:
                if filename.endswith('.csv'):
                    df = pd.read_csv(file_path)
                elif filename.endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(file_path)
                results[filename] = {
                    'columns': df.columns.tolist(),
                    'shape': df.shape,
                    'nulls': df.isnull().sum().to_dict(),
                    'file_url': f'/files/{upload_id}/{filename}',
                    'json_url': f'/files/{upload_id}/{filename}.json'
                }
                logger.debug(f"Procesado {filename}: {df.shape}")
                # Guardar datos completos como JSON
                json_path = os.path.join(upload_path, f"{filename}.json")
                df.to_json(json_path, orient='records', lines=True)
            except Exception as e:
                logger.error(f"Error al procesar {filename}: {str(e)}")
                results[filename] = {'error': f'Failed to process: {str(e)}'}

    # Guardar resumen
    summary_path = os.path.join(upload_path, 'summary.json')
    with open(summary_path, 'w') as f:
        json.dump({'upload_id': upload_id, 'files': results}, f)
    logger.debug(f"Resumen guardado: {summary_path}")

    return jsonify({
        'upload_id': upload_id,
        'files': results,
        'access_url': f'/files/{upload_id}',
        'summary_url': f'/files/{upload_id}/summary.json'
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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)