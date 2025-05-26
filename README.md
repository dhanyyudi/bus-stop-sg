# Bus Stop Data Collector

This repository contains scripts and workflows to collect and merge bus stop data from two sources:

1. LTA DataMall API
2. SimplyGo Website (using web scraping)

## Purpose

The script aims to obtain accurate bus stop name data by:

1. Downloading data from LTA DataMall as the primary data source
2. Supplementing/correcting bus stop names using data from SimplyGo
3. Producing a combined dataset with accurate information

## Repository Structure

```
.
├── .github/
│   └── workflows/
│       └── bus-stop-data.yml     # GitHub Actions workflow
├── data/                         # Folder for storing resulting data
│   ├── lta_datamall_*.csv        # Data from LTA DataMall
│   └── lta_correction.csv        # Merged data result
├── output/                       # Temporary output from scraping process
├── logs/                         # Log files
├── parallelized_simplygo_scraper.py  # Script for SimplyGo scraping
├── bus_stop_data_merger.py       # Main script for collecting and merging data
└── README.md                     # Documentation
```

## Usage

### Local Setup

1. Clone the repository:

   ```bash
   git clone https://github.com/username/bus-stop-data-collector.git
   cd bus-stop-data-collector
   ```

2. Install dependencies:

   ```bash
   pip install selenium beautifulsoup4 pandas tqdm webdriver-manager requests numpy
   ```

3. Run the script:
   ```bash
   python bus_stop_data_merger.py --lta-api-key "YOUR_LTA_API_KEY" --workers 4
   ```

### Parameters

- `--lta-api-key`: API key from LTA DataMall (required)
- `--workers`: Number of parallel workers for SimplyGo scraping (default: 4)
- `--batch-size`: Batch size for saving progress (default: 20)
- `--limit`: Limit number of bus stops to process (optional, for testing)
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR)

### GitHub Actions

This repository uses GitHub Actions to automatically run the data collection process weekly.

To use GitHub Actions:

1. Fork this repository
2. Add the LTA DataMall API key as a repository secret:

   - Go to the repository on GitHub
   - Click Settings > Secrets and variables > Actions
   - Click "New repository secret"
   - Name: `LTA_API_KEY`
   - Value: [Your LTA DataMall API key]
   - Click "Add secret"

3. The workflow will run automatically every Monday at 1 AM, or can be run manually from the Actions tab.

## Output

The script produces several files:

1. `data/lta_datamall_TIMESTAMP.csv`: Original data from LTA DataMall
2. `output/simplygo_bus_stops_TIMESTAMP.csv`: Data from SimplyGo scraping
3. `data/lta_correction_TIMESTAMP.csv`: Merged dataset with corrected bus stop names
4. `data/lta_correction.csv`: Latest file that is always updated with the newest data

## Data Merging Methodology

The data merging process follows these rules:

1. All bus stops from LTA DataMall are used as the base data
2. If a bus stop code exists in SimplyGo data with a valid name, use the name from SimplyGo
3. Otherwise, use the name from LTA DataMall
4. Coordinate and street data always use the data from LTA DataMall

## Contributing

Contributions to improve the scripts or add features are welcome!

1. Fork the repository
2. Create a new branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## Troubleshooting

### Common Issues

1. **Chrome/WebDriver Issues**:

   - Error: `SessionNotCreatedException`
   - Solution: Update Chrome or Chrome WebDriver

2. **API Connection Issues**:

   - Error: `Failed to connect to LTA DataMall API`
   - Solution: Check API key and network connection

3. **Memory Problems**:
   - Error: Browser processes crashing or script running out of memory
   - Solution: Reduce the number of workers or add more memory

### Checking Logs

The script creates detailed logs in the `logs` directory. Check these logs for error messages:

```bash
cat logs/bus_stop_data_*.log | grep ERROR
```

## License
