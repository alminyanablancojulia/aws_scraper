This repository contains the data collection code developed for my Bachelor’s Thesis (TFG) in Business Administration at the Universitat Politècnica de València.
The objective of the project is to perform an exploratory and descriptive analysis of the offer in AWS Marketplace, focusing on product categories, delivery models and pricing structures, using publicly available data only.

Data source:
- The data is collected from public AWS Marketplace webpages, mainly:
- Official sitemap: https://aws.amazon.com/marketplace/sitemap.xml
- Public product detail pages
- Public review pages (when available)
- No private APIs, authentication, or non-public data are used.

What the script does:
- Parses the official AWS Marketplace sitemap to identify the product universe.
- Samples product URLs and extracts information from product pages, including:
- Product name and provider
- Category and category hierarchy
- Delivery model (e.g. SaaS, AMI, container)
- Pricing model (free trial, contract, usage-based, BYOL, etc.)
- Contract duration and price visibility (when available)
- Checks whether products support platform-native reviews and extracts review information when public.
- Exports the resulting dataset to CSV files for analysis in RStudio.
- The script includes request delays and basic error handling to ensure safe data collection.

Repository structure:
aws_scraper/
├── aws_scraper.py   # Main data collection script
├── data/            # Generated CSV files (ignored by Git)
├── .gitignore
└── README.md

Methodological notes:
- The analysis is descriptive, not predictive.
- No conclusions about sales, usage or performance can be drawn due to data limitations.
- Missing values reflect platform design choices, not scraping errors.
- The dataset is intended for statistical analysis and visualization in R.

Author
Julia Almiñana Blanco
Bachelor’s Thesis – Business Administration
Universitat Politècnica de València
