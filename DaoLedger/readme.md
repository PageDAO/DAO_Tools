# DAO Accounting Reports

## Overview

This is a Streamlit-based web application designed to generate comprehensive accounting reports for DAO (Decentralized Autonomous Organization) organizational sub-units. The application connects to the DAO DAO indexer API to fetch proposal data from various blockchain networks (Osmosis, Juno, Stargaze) and processes payment information to create detailed financial reports. The tool enables DAO administrators to track payments, analyze spending patterns across organizational sub-units, and generate accounting summaries for governance and transparency purposes.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture
The application uses Streamlit as the web framework, providing an interactive dashboard with sidebar configuration options. The interface is organized into main content areas for data visualization and reporting, with sidebar controls for API configuration and organizational sub-unit management. The frontend maintains session state to preserve data between user interactions and provides real-time feedback during data processing operations.

### Backend Architecture
The backend follows a modular service-oriented architecture with three main components:

**API Client Layer**: The `DAOAPIClient` class handles all external API communications with the DAO DAO indexer. It manages network requests, response parsing, and error handling for different blockchain networks. The client supports various proposal filtering options and handles different API response formats.

**Data Processing Layer**: The `DataProcessor` class transforms raw proposal data into structured payment information. It extracts payment details from proposal messages, categorizes recipients (core team vs regular), and aggregates data across multiple organizational sub-units. The processor handles complex proposal message formats and base64-encoded data.

**Report Generation Layer**: The `ReportGenerator` class creates various types of accounting reports from processed data. It generates summary statistics, sub-unit breakdowns, and payment analytics using pandas DataFrames for efficient data manipulation.

### Data Flow Architecture
The application follows a linear data pipeline: API data fetching → proposal parsing → payment extraction → report generation → visualization. Data is cached in Streamlit session state to avoid redundant API calls and improve performance.

### Visualization Architecture
The application integrates Plotly for interactive data visualizations, enabling users to explore payment patterns, sub-unit spending distributions, and temporal analysis of DAO financial activities.

## External Dependencies

### Third-party APIs
- **DAO DAO Indexer API**: Primary data source for blockchain proposal information across multiple networks (Osmosis, Juno, Stargaze)

### Python Libraries
- **Streamlit**: Web application framework for the user interface
- **Pandas**: Data manipulation and analysis for processing proposal and payment data
- **Plotly Express/Graph Objects**: Interactive data visualization and charting
- **Requests**: HTTP client library for API communications
 - **ReportLab**: Required to export reports to PDF format. Install with `pip install reportlab` or add to your project's dependencies (e.g., `pyproject.toml` or `requirements.txt`).

### Blockchain Networks
- **Osmosis**: Cosmos-based DEX network for DeFi proposals
- **Juno**: Smart contract platform for governance data
- **Stargaze**: NFT-focused network for creative DAO proposals

### Data Formats
- **JSON**: Primary data exchange format for API responses and configuration
- **Base64**: Encoding format for complex proposal message data