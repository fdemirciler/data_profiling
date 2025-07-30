import json
import logging
import time
import psutil
import traceback
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
import uuid

class ProcessingMonitor:
    """
    Comprehensive monitoring system for real-time data processing pipeline.
    Provides detailed logging, performance metrics, and traceability.
    """
    
    def __init__(self, log_dir: str = "logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.processing_id = str(uuid.uuid4())
        self.setup_logging()
        
    def setup_logging(self):
        """Setup structured logging with JSON formatter."""
        
        # Main processing logger
        self.logger = logging.getLogger('data_processing')
        self.logger.setLevel(logging.DEBUG)
        
        # JSON formatter for structured logs
        class JSONFormatter(logging.Formatter):
            def format(self, record):
                log_entry = {
                    'timestamp': datetime.utcnow().isoformat(),
                    'level': record.levelname,
                    'logger': record.name,
                    'message': record.getMessage(),
                    'processing_id': getattr(record, 'processing_id', None),
                    'node_name': getattr(record, 'node_name', None),
                    'duration_ms': getattr(record, 'duration_ms', None),
                    'memory_usage_mb': getattr(record, 'memory_usage_mb', None),
                    'error_count': getattr(record, 'error_count', 0),
                    'warning_count': getattr(record, 'warning_count', 0),
                    'stack_trace': record.exc_info
                }
                return json.dumps(log_entry)
        
        # File handler for detailed logs
        file_handler = logging.FileHandler(
            self.log_dir / f"processing_{datetime.now().strftime('%Y%m%d')}.log"
        )
        file_handler.setFormatter(JSONFormatter())
        
        # Console handler for real-time monitoring
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(JSONFormatter())
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Processing chain tracker
        self.processing_chain = []
        self.transformations = []
        self.errors = []
        self.warnings = []
        
    def start_processing(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Initialize processing with file metadata."""
        start_time = time.time()
        
        processing_metadata = {
            'processing_id': self.processing_id,
            'file_info': file_info,
            'processing_started': datetime.utcnow().isoformat(),
            'processing_chain': [],
            'overall_quality_score': 0,
            'total_duration_ms': 0
        }
        
        self.logger.info(
            f"Started processing file: {file_info.get('filename', 'unknown')}",
            extra={
                'processing_id': self.processing_id,
                'file_size': file_info.get('file_size', 0),
                'file_type': file_info.get('file_type', 'unknown')
            }
        )
        
        return processing_metadata
        
    def start_node(self, node_name: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Start monitoring a specific processing node."""
        node_start = time.time()
        
        node_status = {
            'node_name': node_name,
            'status': 'started',
            'timestamp': datetime.utcnow().isoformat(),
            'duration_ms': 0,
            'memory_usage_mb': self.get_memory_usage(),
            'error_count': 0,
            'warning_count': 0
        }
        
        self.logger.info(
            f"Starting node: {node_name}",
            extra={
                'processing_id': self.processing_id,
                'node_name': node_name,
                'memory_usage_mb': node_status['memory_usage_mb']
            }
        )
        
        return {
            'node_status': node_status,
            'start_time': node_start,
            'metadata': metadata
        }
        
    def complete_node(self, node_name: str, start_data: Dict[str, Any], 
                     result: Any = None, error: Optional[Exception] = None) -> Dict[str, Any]:
        """Complete monitoring for a processing node."""
        end_time = time.time()
        duration_ms = int((end_time - start_data['start_time']) * 1000)
        
        node_status = start_data['node_status']
        node_status.update({
            'status': 'failed' if error else 'completed',
            'duration_ms': duration_ms,
            'memory_usage_mb': self.get_memory_usage()
        })
        
        if error:
            node_status['error_count'] = 1
            self.errors.append({
                'node': node_name,
                'error_type': type(error).__name__,
                'message': str(error),
                'timestamp': datetime.utcnow().isoformat(),
                'recovery_action': 'best_effort_continuation',
                'severity': 'medium',
                'stack_trace': traceback.format_exc()
            })
            
            self.logger.error(
                f"Node {node_name} failed: {str(error)}",
                extra={
                    'processing_id': self.processing_id,
                    'node_name': node_name,
                    'duration_ms': duration_ms,
                    'error_count': 1
                }
            )
        else:
            self.logger.info(
                f"Node {node_name} completed successfully",
                extra={
                    'processing_id': self.processing_id,
                    'node_name': node_name,
                    'duration_ms': duration_ms
                }
            )
        
        self.processing_chain.append(node_status)
        return node_status
        
    def log_transformation(self, operation: str, description: str, 
                          affected_columns: List[str], before_value: Any = None, 
                          after_value: Any = None, success: bool = True, 
                          error_message: str = None):
        """Log a data transformation for audit trail."""
        transformation = {
            'operation': operation,
            'description': description,
            'timestamp': datetime.utcnow().isoformat(),
            'affected_columns': affected_columns,
            'before_value': before_value,
            'after_value': after_value,
            'success': success,
            'error_message': error_message
        }
        
        self.transformations.append(transformation)
        
        log_level = logging.WARNING if not success else logging.INFO
        self.logger.log(log_level, 
            f"Transformation: {operation} - {description}",
            extra={
                'processing_id': self.processing_id,
                'operation': operation,
                'success': success
            }
        )
        
    def log_warning(self, node_name: str, message: str, suggested_action: str = None):
        """Log a warning with suggested action."""
        warning = {
            'node': node_name,
            'message': message,
            'timestamp': datetime.utcnow().isoformat(),
            'suggested_action': suggested_action
        }
        
        self.warnings.append(warning)
        
        self.logger.warning(
            f"Warning in {node_name}: {message}",
            extra={
                'processing_id': self.processing_id,
                'node_name': node_name,
                'warning_count': 1
            }
        )
        
    def get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024
        
    def finalize_processing(self, start_time: float, quality_scores: Dict[str, float]) -> Dict[str, Any]:
        """Finalize processing and generate comprehensive report."""
        end_time = time.time()
        total_duration_ms = int((end_time - start_time) * 1000)
        
        final_report = {
            'metadata': {
                'processing_id': self.processing_id,
                'processing_completed': datetime.utcnow().isoformat(),
                'total_duration_ms': total_duration_ms,
                'processing_chain': self.processing_chain,
                'overall_quality_score': quality_scores.get('overall_score', 0),
                'final_memory_usage_mb': self.get_memory_usage()
            },
            'audit': {
                'transformations': self.transformations,
                'errors': self.errors,
                'warnings': self.warnings,
                'total_transformations': len(self.transformations),
                'total_errors': len(self.errors),
                'total_warnings': len(self.warnings)
            },
            'quality': quality_scores
        }
        
        self.logger.info(
            "Processing completed",
            extra={
                'processing_id': self.processing_id,
                'total_duration_ms': total_duration_ms,
                'total_errors': len(self.errors),
                'total_warnings': len(self.warnings),
                'overall_quality_score': quality_scores.get('overall_score', 0)
            }
        )
        
        # Save detailed report to file
        report_file = self.log_dir / f"report_{self.processing_id}.json"
        with open(report_file, 'w') as f:
            json.dump(final_report, f, indent=2, default=str)
            
        return final_report
        
    def get_processing_status(self) -> Dict[str, Any]:
        """Get real-time processing status."""
        return {
            'processing_id': self.processing_id,
            'current_node': self.processing_chain[-1]['node_name'] if self.processing_chain else None,
            'completed_nodes': len([n for n in self.processing_chain if n['status'] == 'completed']),
            'failed_nodes': len([n for n in self.processing_chain if n['status'] == 'failed']),
            'total_errors': len(self.errors),
            'total_warnings': len(self.warnings),
            'current_memory_usage_mb': self.get_memory_usage()
        }
