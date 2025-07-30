# Analysis Agent - Enhanced Edition

An AI-powered data analysis tool with **enhanced data processing pipeline** that allows you to upload CSV/Excel files and ask questions about your data using natural language. Built with FastAPI, React, and Google's Gemini AI, featuring advanced type inference, layout detection, and comprehensive data quality assessment.

## 📋 Latest Updates (Version 2.0 - 2025-07-30)

### ✅ Multi-Format Excel Processing
- **Excel Multi-Sheet Processing**: Full support for Excel files with multiple sheets
- **Enhanced Excel Export**: Saves cleaned data as new Excel files (not CSV)
- **Sheet-by-Sheet Processing**: Each sheet processed independently with full cleaning
- **Currency Detection**: Advanced pattern recognition for financial data
- **Subsection Header Removal**: Automatically removes section headers like "Assets", "Liabilities", "Revenue"

### ✅ Production-Ready Features
- **File Size Validation**: Up to 50MB with automatic chunking
- **Comprehensive Error Handling**: Best-effort processing with graceful degradation
- **Detailed Processing Reports**: JSON-formatted audit trails
- **Real-time Processing**: Immediate processing on file upload
- **Structured Logging**: Complete audit trails for all transformations

### ✅ Enhanced Data Cleaning
- **Subsection Header Detection**: Smart identification of data vs. header rows
- **Currency Symbol Cleaning**: Removes $, commas, converts to numeric
- **Blank Row Removal**: Automatic removal of empty rows
- **Type-Aware Cleaning**: Strategies based on detected column types
- **Quality Scoring**: Automatic assessment (0-100 scale)

## 🚀 Enhanced Features

### **Advanced Data Processing Pipeline**
- **🧠 Smart Type Inference**: Automatically detects currency, percentages, dates, periods, IDs, and text columns with confidence scoring
- **📊 Layout Detection**: Recognizes wide vs. long format data and normalizes automatically
- **🧹 Intelligent Data Cleaning**: Type-aware cleaning with configurable options
- **📈 Comprehensive Data Profiling**: Statistical analysis, correlation detection, and quality assessment
- **📋 Complete Audit Trail**: Full logging of all data transformations with timestamps
- **🔄 Multi-Sheet Support**: Excel files with multiple sheets processed individually
- **⚡ Chunked Processing**: Efficient handling of large datasets with memory management
- **🛡️ Robust Error Recovery**: Multiple fallback levels for reliable processing

### **Core Features**
- **File Upload**: Support for CSV, XLSX, and XLS files (up to 50MB) with enhanced processing
- **Natural Language Queries**: Ask questions about your data in plain English
- **AI-Powered Analysis**: Uses Gemini 2.5 Flash for intelligent data interpretation
- **Enhanced Tool Architecture**: Advanced modular tools with backward compatibility
- **Session Management**: Multi-session support with enhanced pipeline results storage
- **Modern UI**: Clean, responsive interface built with React and Tailwind CSS
- **Data Quality Scoring**: Automatic assessment of data quality (0-100 scale)
- **Structured Logging**: JSON-formatted logging with comprehensive audit trails

## 🚀 Enhanced Features

### **Advanced Data Processing Pipeline**
- **🧠 Smart Type Inference**: Automatically detects currency, percentages, dates, periods, IDs, and text columns with confidence scoring
- **📊 Layout Detection**: Recognizes wide vs. long format data and normalizes automatically
- **🧹 Intelligent Data Cleaning**: Type-aware cleaning strategies with configurable options
- **📈 Comprehensive Data Profiling**: Statistical analysis, correlation detection, and quality assessment
- **📋 Complete Audit Trail**: Full logging of all data transformations with timestamps
- **🔄 Multi-Sheet Support**: Excel files with multiple sheets processed individually
- **⚡ Chunked Processing**: Efficient handling of large datasets with memory management
- **🛡️ Robust Error Recovery**: Multiple fallback levels for reliable processing

### **Core Features**
- **File Upload**: Support for CSV, XLSX, and XLS files (up to 50MB) with enhanced processing
- **Natural Language Queries**: Ask questions about your data in plain English
- **AI-Powered Analysis**: Uses Gemini 2.5 Flash for intelligent data interpretation
- **Enhanced Tool Architecture**: Advanced modular tools with backward compatibility
- **Session Management**: Multi-session support with enhanced pipeline results storage
- **Modern UI**: Clean, responsive interface built with React and Tailwind CSS
- **Data Quality Scoring**: Automatic assessment of data quality (0-100 scale)
- **Structured Logging**: JSON-formatted logging with comprehensive audit trails

## 🏗️ Enhanced Architecture

```
[User] → [React Frontend] → [FastAPI Backend] → [Enhanced Orchestrator] → [Pipeline Processor] → [Enhanced Tools] → [Gemini LLM] → [Response]
                                                                      ↓
                                                           [Type Inference] → [Layout Detection] → [Smart Cleaning] → [Quality Assessment]
```

### Enhanced Components

1. **Enhanced Pipeline** (`pipeline/`): Advanced data processing engine
   - **Pipeline Orchestrator**: Main processing coordinator
   - **Type Inferencer**: Smart semantic type detection
   - **Layout Detector**: Wide/long format recognition and normalization
   - **Data Cleaner**: Type-aware cleaning with configurable strategies
   - **Data Profiler**: Comprehensive statistical analysis and quality metrics
   - **Audit Logger**: Complete transformation tracking

2. **Enhanced Data Processor** (`backend/services/`): Integration layer
   - **Memory Processing**: Efficient in-memory data handling
   - **Chunked Processing**: Large file support with memory management
   - **Error Recovery**: Multi-level fallback processing
   - **Quality Assessment**: Automatic data quality scoring and reporting

3. **Enhanced Tools** (`backend/tools/enhanced_tools.py`): Advanced analysis tools
   - **Enhanced Data Cleaner**: Type-aware cleaning with period detection
   - **Enhanced Data Profiler**: Statistical analysis with quality metrics
   - **Enhanced Preprocessor**: Smart type inference with confidence scoring

4. **Enhanced Session Management**: Pipeline results storage and multi-sheet support

## 🛠️ Technology Stack

### Backend
- **FastAPI** - Modern Python web framework
- **Pandas** - Data manipulation and analysis
- **Google Generative AI** - Gemini LLM integration
- **Pydantic** - Data validation and settings management
- **Uvicorn** - ASGI server
- **Pytest** - Testing framework with async support

### Frontend
- **React** - UI framework
- **Tailwind CSS** - Utility-first CSS framework
- **Axios** - HTTP client

## 🚀 Enhanced Pipeline Capabilities

### 🧠 Smart Type Inference
The enhanced pipeline includes advanced type detection that goes beyond basic data types:

- **Semantic Types**: Currency, percentage, date, period (years), ID, text
- **Confidence Scoring**: Each type detection includes confidence levels
- **Context Awareness**: Column names and data patterns inform type decisions
- **Period Detection**: Automatically identifies year columns (2022, 2023, etc.) as periods
- **Multi-Format Support**: Handles various date formats, currency symbols, and percentage notations

### 📊 Layout Intelligence
Automatically detects and handles different data layouts:

- **Wide Format**: Metrics as columns (Q1, Q2, Q3, Q4 or 2022, 2023, 2024)
- **Long Format**: Time series data with date/period columns
- **Auto-Normalization**: Converts wide format to long format when beneficial
- **Header Detection**: Smart identification of data vs. header rows

### 🧹 Type-Aware Cleaning
Advanced cleaning strategies based on detected column types:

- **Currency Cleaning**: Removes symbols, handles negative values in parentheses
- **Percentage Normalization**: Converts percentages to decimal format
- **Date Standardization**: Consistent date format across the dataset
- **Missing Value Handling**: Type-specific strategies for null values
- **Outlier Detection**: Statistical methods for identifying anomalous values

### 📈 Comprehensive Quality Assessment
Automated data quality evaluation with detailed reporting:

- **Quality Score**: Overall 0-100 quality rating
- **Missing Value Analysis**: Percentage and pattern of missing data
- **Data Type Consistency**: Validation of inferred types across rows
- **Statistical Summary**: Mean, median, std dev, quartiles for numeric data
- **Correlation Matrix**: Relationship analysis between numeric columns

### 🔄 Multi-Sheet Excel Support
Enhanced handling of complex Excel files:

- **Sheet Detection**: Automatically identifies and processes all sheets
- **Individual Processing**: Each sheet processed with its own type inference
- **Combined Results**: Consolidated analysis across all sheets
- **Sheet-Specific Quality**: Individual quality scores per sheet

### ⚡ Performance Optimizations
Efficient processing for large datasets:

- **Chunked Processing**: Memory-efficient handling of large files
- **Streaming Analysis**: Process data without loading entire file into memory
- **Incremental Quality Assessment**: Real-time quality metrics during processing
- **Parallel Processing**: Multi-threaded type inference and cleaning operations

### 🛡️ Robust Error Recovery
Multiple fallback levels ensure reliable processing:

1. **Enhanced Pipeline**: Full type-aware processing
2. **Chunked Processing**: Fallback for memory-intensive datasets
3. **Individual Processing**: Per-column fallback for problematic data
4. **Legacy Processing**: Basic cleaning as final fallback
5. **Error Reporting**: Detailed error logs with specific failure points

### 📋 Complete Audit Trail
Comprehensive logging of all data transformations:

- **Transformation Log**: Step-by-step record of all changes
- **Type Inference Results**: Confidence scores and detection reasoning
- **Quality Metrics**: Before/after quality assessments
- **Error Documentation**: Complete error logs with context
- **Performance Metrics**: Processing time and memory usage tracking

## 🧪 Testing & Quality Assurance

### Unit Testing Framework
Comprehensive testing suite for all data processing components:

```bash
# Run all unit tests
cd tests/unit_tests
python -m pytest -v

# Run specific test categories
python -m pytest test_type_inference.py -v
python -m pytest test_data_quality.py -v
python -m pytest test_currency_cleaning.py -v

# Run with coverage
python -m pytest --cov=../.. --cov-report=html
```

### Test Categories
- **Type Inference Tests**: Currency, percentage, numeric detection
- **Data Quality Tests**: Completeness, uniqueness, outlier detection
- **Currency Cleaning Tests**: Symbol removal, format conversion
- **Edge Case Testing**: Empty data, invalid inputs, boundary conditions

### Test Structure
```
tests/
├── unit_tests/
│   ├── pytest.ini              # pytest configuration
│   ├── conftest.py            # test fixtures & sample data
│   ├── test_type_inference.py # type detection tests
│   ├── test_data_quality.py   # quality metrics tests
│   ├── test_currency_cleaning.py # currency/percentage tests
│   └── run_tests.py          # test runner script
├── data_tests/               # sample data & results
└── integration_tests/        # end-to-end tests
```

### Test Results Summary
- **✅ Type Inference**: Currency, percentage, numeric detection working
- **✅ Data Quality**: Completeness calculation functional
- **✅ Currency Cleaning**: Symbol removal and format conversion verified
- **✅ Framework Status**: Production-ready testing infrastructure
- **✅ Sample Data**: Consistent test fixtures for reliable testing

