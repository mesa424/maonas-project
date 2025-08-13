# Maonas Database Explorer

A comprehensive web application for exploring and analyzing maritime historical data from the Mediterranean region during the late medieval and early modern periods.

## Features

- **Database Overview**: Browse and search through main database tables
- **Social Network Analysis**: Analyze relationships between individuals and organizations
- **Economic Analysis**: Statistical and econometric analysis of price data and market dynamics
- **Professional Interface**: Clean, academic design with Times New Roman typography
- **Team Management**: Information about project contributors and institutional partners

## Project Structure

```
maonas-database-app/
├── main_app.py              # Main Streamlit application
├── config.py                # Configuration settings and mappings
├── database_manager.py      # Database operations and queries
├── styles.py                # CSS styling and theming
├── utils.py                 # Utility functions and helpers
├── requirements.txt         # Project dependencies
├── README.md               # Project documentation
└── assets/                 # Static assets
    ├── merchant_tile.jpg   # Main logo
    └── partner_logos/      # Partner institution logos
```

## Installation

1. **Clone the repository**
   ```bash
   git clone [repository-url]
   cd maonas-database-app
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure database connection**
   
   Edit `config.py` to match your MySQL database settings:
   ```python
   DB_CONFIG = {
       'host': 'localhost',
       'port': 3306,
       'database': 'maonas',
       'username': 'your_username',
       'password': 'your_password'
   }
   ```

5. **Add assets**
   
   Create an `assets` folder and add:
   - `merchant_tile.jpg` - Main application logo
   - Partner logos in PNG/JPG format

## Usage

1. **Start the application**
   ```bash
   streamlit run main_app.py
   ```

2. **Access the application**
   
   Open your web browser and navigate to `http://localhost:8501`

## Database Tables

The application provides access to the following main tables:

- **Individuals (IMT)**: Personal records and biographical information
- **Legal Acts**: Historical legal documents and contracts
- **Vessels**: Maritime vessel records and specifications
- **Organizations**: Institutional and organizational entities
- **Goods' Prices**: Commodity pricing data
- **Exchange Rates**: Currency exchange information
- **Equivalencies**: Unit conversion data

## Pages Overview

### Home Page
- Project description and context
- Navigation to other sections
- Partner institution logos

### Basic Database Overview
- Tabbed interface for different data tables
- Search functionality within tables
- Data export capabilities
- Formatted column names for user-friendly display

### Team Page
- Project team member information
- Contact details and institutional affiliations
- Partner organization information

### Analysis Tools
- Social Network Analysis tools (to be implemented)
- Economic Analysis tools (to be implemented)
- Comprehensive measurement capabilities

## Configuration

### Adding Team Members
Edit the `TEAM_MEMBERS` list in `config.py`:
```python
TEAM_MEMBERS = [
    {
        'name': 'Dr. Jane Smith',
        'title': 'Principal Investigator',
        'affiliation': 'University Name',
        'email': 'jane.smith@university.edu',
        'website': 'https://university.edu/jane-smith'
    }
]
```

### Customizing Table Display
Column names can be customized in the `COLUMN_DISPLAY_NAMES` dictionary in `config.py`.

### Styling
The application uses Times New Roman font throughout. Styling can be modified in `styles.py`.

## Future Development

The current version provides the structural framework and basic database access. Future development will include:

1. **Social Network Analysis Implementation**
   - Network visualization tools
   - Centrality measures calculation
   - Community detection algorithms

2. **Economic Analysis Implementation**
   - Time series analysis
   - Price elasticity calculations
   - Market integration tests
   - Volatility modeling (GARCH)

3. **Advanced Features**
   - Export functionality for analysis results
   - Custom query builder
   - Data filtering and aggregation tools

## Dependencies

- **Streamlit**: Web application framework
- **MySQL Connector**: Database connectivity
- **Pandas**: Data manipulation and analysis
- **Plotly**: Interactive visualizations
- **NetworkX**: Network analysis (for future features)
- **Statsmodels**: Statistical analysis (for future features)

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-feature`)
3. Commit your changes (`git commit -am 'Add new feature'`)
4. Push to the branch (`git push origin feature/new-feature`)
5. Create a Pull Request

## License

[Specify your license here]

## Contact

For questions about the database or technical support, please contact the project team through the information provided in the Team page of the application.

## Acknowledgments

This project is supported by [list supporting institutions and funding sources].