from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from typing import Dict, Any, List
import os
import uuid
import json
from pathlib import Path
import logging
from datetime import datetime
import asyncio

from ..processors.enhanced_preprocessor import EnhancedDataPreprocessor
from ..monitoring.processing_monitor import ProcessingMonitor

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="Data Processing API", version="1.0.0")

# Global preprocessor instance
preprocessor = EnhancedDataPreprocessor()

# File storage configuration
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)
PROCESSED_DIR = Path("processed")
PROCESSED_DIR.mkdir(exist_ok=True)

# Processing status storage
processing_status = {}

@app.post("/upload/")
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
) -> Dict[str, Any]:
    """
    Upload and immediately process file in real-time.
    Returns processing ID for status tracking.
    """
    
    try:
        # Validate file
        if not file.filename:
            raise HTTPException(status_code=400, detail="No file provided")
        
        # Check file extension
        allowed_extensions = {'.csv', '.xlsx', '.xls'}
        file_extension = Path(file.filename).suffix.lower()
        
        if file_extension not in allowed_extensions:
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported file type: {file_extension}. Use: {allowed_extensions}"
            )
        
        # Check file size (50MB limit)
        content = await file.read()
        if len(content) > 50 * 1024 * 1024:
            raise HTTPException(status_code=400, detail="File too large (max 50MB)")
        
        # Generate processing ID
        processing_id = str(uuid.uuid4())
        
        # Save file
        file_path = UPLOAD_DIR / f"{processing_id}_{file.filename}"
        with open(file_path, 'wb') as f:
            f.write(content)
        
        # File info
        file_info = {
            'filename': file.filename,
            'file_size': len(content),
            'file_type': file_extension.lstrip('.'),
            'upload_timestamp': datetime.utcnow().isoformat(),
            'processing_id': processing_id
        }
        
        # Start background processing
        background_tasks.add_task(
            process_file_async,
            str(file_path),
            file_info,
            processing_id
        )
        
        # Store initial status
        processing_status[processing_id] = {
            'status': 'processing',
            'started_at': datetime.utcnow().isoformat(),
            'file_info': file_info,
            'progress': 0
        }
        
        return {
            'processing_id': processing_id,
            'status': 'processing_started',
            'message': 'File uploaded successfully. Processing in background.',
            'estimated_time': '5-30 seconds depending on file size'
        }
        
    except Exception as e:
        logger.error(f"Upload error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/status/{processing_id}")
async def get_processing_status(processing_id: str) -> Dict[str, Any]:
    """Get real-time processing status."""
    
    if processing_id not in processing_status:
        raise HTTPException(status_code=404, detail="Processing ID not found")
    
    status = processing_status[processing_id]
    
    # If processing is complete, return full results
    if status['status'] == 'completed':
        return {
            'processing_id': processing_id,
            'status': 'completed',
            'file_info': status['file_info'],
            'results': status.get('results', {}),
            'processing_time': status.get('processing_time', 0),
            'quality_score': status.get('quality_score', 0)
        }
    
    # Return current processing status
    return {
        'processing_id': processing_id,
        'status': status['status'],
        'file_info': status['file_info'],
        'progress': status.get('progress', 0),
        'current_step': status.get('current_step', 'unknown')
    }

@app.get("/results/{processing_id}")
async def get_processing_results(processing_id: str) -> Dict[str, Any]:
    """Get complete processing results."""
    
    if processing_id not in processing_status:
        raise HTTPException(status_code=404, detail="Processing ID not found")
    
    status = processing_status[processing_id]
    
    if status['status'] != 'completed':
        return {
            'processing_id': processing_id,
            'status': status['status'],
            'message': 'Processing not yet complete'
        }
    
    return {
        'processing_id': processing_id,
        'status': 'completed',
        'file_info': status['file_info'],
        'results': status.get('results', {}),
        'processing_time': status.get('processing_time', 0),
        'quality_score': status.get('quality_score', 0),
        'download_url': f"/download/{processing_id}"
    }

@app.get("/download/{processing_id}")
async def download_processed_file(processing_id: str):
    """Download processed data file."""
    
    if processing_id not in processing_status:
        raise HTTPException(status_code=404, detail="Processing ID not found")
    
    status = processing_status[processing_id]
    
    if status['status'] != 'completed':
        raise HTTPException(status_code=400, detail="Processing not yet complete")
    
    processed_file = PROCESSED_DIR / f"{processing_id}_processed.json"
    
    if not processed_file.exists():
        raise HTTPException(status_code=404, detail="Processed file not found")
    
    return JSONResponse(
        content=json.loads(processed_file.read_text()),
        media_type="application/json"
    )

@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """Health check endpoint."""
    
    return {
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'active_processings': len([s for s in processing_status.values() if s['status'] == 'processing']),
        'completed_processings': len([s for s in processing_status.values() if s['status'] == 'completed'])
    }

@app.get("/logs/{processing_id}")
async def get_processing_logs(processing_id: str) -> Dict[str, Any]:
    """Get detailed processing logs."""
    
    if processing_id not in processing_status:
        raise HTTPException(status_code=404, detail="Processing ID not found")
    
    status = processing_status[processing_id]
    
    # Read processing logs
    log_file = Path("logs") / f"report_{processing_id}.json"
    
    if log_file.exists():
        with open(log_file, 'r') as f:
            logs = json.load(f)
    else:
        logs = {'error': 'Logs not yet available'}
    
    return {
        'processing_id': processing_id,
        'logs': logs
    }

@app.get("/monitoring/dashboard")
async def get_monitoring_dashboard() -> Dict[str, Any]:
    """Get real-time monitoring dashboard data."""
    
    total_processings = len(processing_status)
    completed = len([s for s in processing_status.values() if s['status'] == 'completed'])
    processing = len([s for s in processing_status.values() if s['status'] == 'processing'])
    failed = len([s for s in processing_status.values() if s['status'] == 'failed'])
    
    avg_quality_score = 0
    if completed > 0:
        scores = [s.get('quality_score', 0) for s in processing_status.values() 
                  if s['status'] == 'completed']
        avg_quality_score = sum(scores) / len(scores) if scores else 0
    
    return {
        'total_processings': total_processings,
        'completed': completed,
        'processing': processing,
        'failed': failed,
        'avg_quality_score': round(avg_quality_score, 2),
        'system_status': 'healthy',
        'uptime': 'running'
    }

async def process_file_async(file_path: str, file_info: Dict[str, Any], 
                           processing_id: str):
    """
    Background task for file processing.
    Handles real-time processing with comprehensive monitoring.
    """
    
    try:
        # Update status
        processing_status[processing_id].update({
            'status': 'processing',
            'progress': 10,
            'current_step': 'file_ingestion'
        })
        
        # Process file
        result = preprocessor.process_file(file_path, file_info)
        
        # Save processed data
        processed_file = PROCESSED_DIR / f"{processing_id}_processed.json"
        with open(processed_file, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        
        # Update final status
        processing_status[processing_id].update({
            'status': 'completed',
            'completed_at': datetime.utcnow().isoformat(),
            'results': result,
            'quality_score': result.get('quality', {}).get('overall_score', 0),
            'processing_time': result.get('metadata', {}).get('total_duration_ms', 0)
        })
        
        # Cleanup uploaded file
        Path(file_path).unlink(missing_ok=True)
        
    except Exception as e:
        logger.error(f"Processing failed for {processing_id}: {str(e)}")
        
        processing_status[processing_id].update({
            'status': 'failed',
            'error': str(e),
            'failed_at': datetime.utcnow().isoformat()
        })

# WebSocket endpoint for real-time updates (optional)
@app.websocket("/ws/{processing_id}")
async def websocket_endpoint(websocket, processing_id: str):
    """WebSocket endpoint for real-time processing updates."""
    
    await websocket.accept()
    
    try:
        while True:
            if processing_id in processing_status:
                status = processing_status[processing_id]
                await websocket.send_json(status)
                
                if status['status'] in ['completed', 'failed']:
                    break
                    
            await asyncio.sleep(1)
            
    except Exception as e:
        logger.error(f"WebSocket error: {str(e)}")
    finally:
        await websocket.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
