# Therapy Python ETL

A Python-based ETL (Extract, Transform, Load) pipeline designed to process therapy-related data from Excel files and load them into MongoDB. This project handles eligibility data, claim reports, and provider claims with support for data encryption using AWS KMS.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [ETL Modules](#etl-modules)
- [Input Files](#input-files)
- [Development](#development)
- [Troubleshooting](#troubleshooting)

## Overview

This ETL pipeline automates the process of:
1. **Extracting** data from Excel files (.xlsx, .xls)
2. **Transforming** data according to business rules and data models
3. **Loading** processed data into MongoDB collections

The system supports three main data processing pipelines:
- **Eligibility**: Patient eligibility and enrollment data
- **Claim Reports**: Insurance claim reporting data
- **Provider Claims**: Healthcare provider claim information

## Features

- ✅ Command-line interface using Click
- ✅ Batch processing of Excel files
- ✅ MongoDB integration with connection pooling
- ✅ AWS KMS encryption support
- ✅ Data validation and transformation
- ✅ Modular ETL architecture
- ✅ Support for both modern (.xlsx) and legacy (.xls) Excel formats
- ✅ Configurable environment-based settings

## Project Structure

```
therapy-python-etl/
├── main.py                      # Entry point with CLI interface
├── requirements.txt             # Python dependencies
├── .env                         # Environment configuration (not in git)
├── .gitignore                  # Git ignore rules
├── input-files/                # Source data files
│   ├── eligibility/           # Eligibility Excel files
│   ├── claim_rpt/             # Claim report Excel files
│   └── provider_claims/       # Provider claim Excel files
├── src/
│   ├── config/                # Configuration modules
│   │   ├── config.py         # Application configuration
│   │   └── mapper.py         # Data mapping configurations
│   ├── core/
│   │   ├── command/          # CLI command handlers
│   │   │   └── etl.py       # ETL command execution logic
│   │   ├── migrate/          # ETL migration modules
│   │   │   ├── base_etl.py              # Base ETL class
│   │   │   ├── eligibility.py           # Eligibility ETL
│   │   │   ├── claim_rpt.py             # Claim report ETL
│   │   │   └── provider_claim.py        # Provider claim ETL
│   │   ├── service/          # Business logic services
│   │   │   ├── eligibility/
│   │   │   ├── subscribers/
│   │   │   ├── patients/
│   │   │   ├── enrollees/
│   │   │   └── provider_claims/
│   │   └── data_frame_type/  # DataFrame type definitions
│   ├── shared/
│   │   ├── base/             # Base classes and models
│   │   ├── constant/         # Application constants
│   │   ├── helper/           # Helper utilities
│   │   │   └── mongodb_helper.py
│   │   ├── interface/        # Interface definitions
│   │   └── utils/            # Utility functions
│   └── mapper/               # Data mapping logic
└── venv/                     # Virtual environment (not in git)
```

## Prerequisites

- **Python**: 3.10 or higher (uses match-case statements)
- **MongoDB**: Access to a MongoDB cluster (local or cloud)
- **AWS Account**: For KMS encryption features (optional)
- **Excel Files**: Input data files in .xlsx or .xls format

## Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd therapy-python-etl
```

### 2. Create Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate

# On Windows:
venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Set Up Environment Variables

Create a `.env` file in the project root (copy from `.env.example` if available):

```bash
cp .env.example .env
```

Then edit `.env` with your configuration (see [Configuration](#configuration) section).

## Configuration

Edit the `.env` file with your environment-specific settings:

### MongoDB Configuration

```env
MONGO_DB_PORT=                  # Optional: MongoDB port (default uses SRV)
MONGO_DB_NAME=dev-therapy       # Database name
MONGO_CLUSTER_NAME=cluster0.xxx # MongoDB cluster name
MONGO_USERNAME=your_username    # MongoDB username
MONGO_PASSWORD=your_password    # MongoDB password
MONGO_DB_PROTOCOL=mongodb+srv   # Protocol (mongodb or mongodb+srv)
```

### Key Vault Configuration (Optional)

```env
MONGO_AUTO_ENCRYPTION_SHARED_LIB_PATH=
KEY_VAULT_COLLECTION_NAME=keyVaults
KEY_VAULT_DATA_KEY_NAME=dev-dek
```

### AWS Configuration (Optional)

```env
AWS_ACCESS_KEY=your_access_key
AWS_SECRET_KEY=your_secret_key
AWS_KEY_ARN=arn:aws:kms:region:account:key/key-id
AWS_REGION=us-east-1
```

**⚠️ Security Note**: Never commit the `.env` file to version control. It contains sensitive credentials.

## Usage

### Basic Commands

The ETL pipeline is executed via the command-line interface using the `main.py` script.

#### Run All ETL Processes

```bash
python main.py --execute ALL
```

or simply:

```bash
python main.py
```

(ALL is the default option)

#### Run Specific ETL Process

**Eligibility ETL Only:**
```bash
python main.py --execute ELIGIBILITY
```

**Claim Report ETL Only:**
```bash
python main.py --execute CLAIM_RPT
```

**Provider Claim ETL Only:**
```bash
python main.py --execute PROVIDER_CLAIM
```

**Fix subscriber name in patient Only:**
```bash
python main.py --execute PATIENT_FIX_SUBSCRIBER_NAME
```

**Fix product name and patient formatted date on eligibility Only:**
```bash
python main.py --execute ELIGIBILITY_FIX_PRODUCT_AND_PATIENT_DOB_PATCH
```

### Command Options

```bash
python main.py --help
```

**Available Options:**

- `-exec, --execute`: Choose which ETL to execute
  - `ALL` - Run all ETL processes (default)
  - `ELIGIBILITY` - Process eligibility data only
  - `CLAIM_RPT` - Process claim reports only
  - `PROVIDER_CLAIM` - Process provider claims only

### Example Usage

```bash
# Activate virtual environment first
source venv/bin/activate

# Run eligibility ETL
python main.py -exec ELIGIBILITY

# Run all ETL processes
python main.py --execute ALL
```

## ETL Modules

### 1. Eligibility ETL

**Purpose**: Processes patient eligibility and enrollment data

**Input**: Excel files in `input-files/eligibility/`

**Collections Updated**:
- Subscribers
- Patients
- Enrollees
- Eligibility records

**Features**:
- Validates patient information
- Processes enrollment dates
- Handles gender and name normalization
- Manages subscriber relationships

### 2. Claim Report ETL

**Purpose**: Processes insurance claim reporting data

**Input**: Excel files in `input-files/claim_rpt/`

**Collections Updated**:
- Claim reports
- Related claim data

**Features**:
- Claim validation
- Report generation
- Data quality checks

### 3. Provider Claim ETL

**Purpose**: Processes healthcare provider claim information

**Input**: Excel files in `input-files/provider_claims/`

**Collections Updated**:
- Provider claims
- Provider information
- Claim details

**Features**:
- Provider data validation
- Claim processing
- Service qualifier mapping

## Input Files

### File Structure

Place your Excel files in the appropriate subdirectory:

```
input-files/
├── eligibility/        # Place eligibility .xlsx or .xls files here
├── claim_rpt/          # Place claim report files here
└── provider_claims/    # Place provider claim files here
```

### Supported Formats

- `.xlsx` - Modern Excel format (Excel 2007+)
- `.xls` - Legacy Excel format (Excel 97-2003)

### File Requirements

- Files must follow the expected schema for each ETL type
- Column headers should match the expected field names
- Date formats should be consistent
- Required fields must not be empty

## Development

### Project Dependencies

- **pandas**: Data manipulation and analysis
- **numpy**: Numerical computing
- **openpyxl**: Read/write Excel 2010 xlsx/xlsm files
- **xlrd**: Read legacy Excel .xls files
- **pymongo**: MongoDB driver for Python
- **python-dotenv**: Environment variable management
- **click**: Command-line interface creation
- **uuid_utils**: UUID generation and handling

### Adding a New ETL Module

1. Create a new ETL class in `src/core/migrate/`
2. Inherit from `BaseETL`
3. Implement the `execute()` method
4. Add the new ETL option to `src/core/command/etl.py`
5. Update the CLI choices in `main.py`

### Code Style

- Follow PEP 8 guidelines
- Use type hints where appropriate
- Keep functions focused and modular
- Document complex business logic

## Troubleshooting

### Common Issues

**Issue**: `ModuleNotFoundError`
```bash
Solution: Ensure virtual environment is activated and dependencies are installed
source venv/bin/activate
pip install -r requirements.txt
```

**Issue**: MongoDB connection fails
```bash
Solution: 
1. Check .env file has correct MongoDB credentials
2. Verify MongoDB cluster is accessible
3. Check network connectivity and firewall rules
4. Ensure IP address is whitelisted in MongoDB Atlas
```

**Issue**: Excel file not found
```bash
Solution:
1. Verify files are in correct input-files subdirectory
2. Check file permissions
3. Ensure file names don't contain special characters
```

**Issue**: Data validation errors
```bash
Solution:
1. Review Excel file schema matches expected format
2. Check for missing required fields
3. Validate date formats
4. Review error logs for specific field issues
```

### Logging

The application prints processing information to the console. Monitor the output for:
- Files being processed
- Record counts
- Validation errors
- Success/failure messages

### Getting Help

For issues or questions:
1. Check the error message and logs
2. Review the troubleshooting section
3. Verify configuration settings
4. Check input data format

## License

[Add your license information here]

## Contributors

[Add contributor information here]

---

**Last Updated**: February 2026
