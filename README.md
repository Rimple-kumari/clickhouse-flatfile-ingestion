# ClickHouse-FlatFile Data Ingestion Tool

A web-based application that facilitates bidirectional data ingestion between ClickHouse database and Flat Files.

## Features

- Bidirectional data flow (ClickHouse ↔ Flat File)
- JWT token-based authentication for ClickHouse
- Column selection for data ingestion
- Progress tracking and record counting
- User-friendly interface
- Support for multi-table joins (bonus feature)

## Prerequisites

- Python 3.8+
- Node.js 14+
- ClickHouse server (local or remote)
- Docker (optional, for running ClickHouse locally)

## Project Structure

```
.
├── backend/           # FastAPI backend
├── frontend/         # React frontend
├── docker/           # Docker configuration
└── README.md         # This file
```

## Setup Instructions

### Backend Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate
```

2. Install dependencies:
```bash
cd backend
pip install -r requirements.txt
```

3. Start the backend server:
```bash
uvicorn main:app --reload
```

### Frontend Setup

1. Install dependencies:
```bash
cd frontend
npm install
```

2. Start the development server:
```bash
npm start
```

### Running with Docker

1. Build and start the containers:
```bash
docker-compose up --build
```

2. Access the application at http://localhost:3000

## Configuration

1. Backend configuration is in `backend/config.py`
2. Frontend configuration is in `frontend/src/config.js`

## Testing

1. Backend tests:
```bash
cd backend
pytest
```

2. Frontend tests:
```bash
cd frontend
npm test
```

## Copyright and Attribution

© 2024 Rimple. All rights reserved.

This application is developed and maintained by Rimple. The code, design, and documentation are proprietary and confidential. Unauthorized copying, modification, distribution, or use of this software, via any medium, is strictly prohibited without explicit permission from the author.

## Contact

For any queries or support, please contact the developer. 