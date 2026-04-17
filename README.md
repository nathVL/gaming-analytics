# Steam & Twitch Data Platform

## Project Overview

This project is a modern data platform designed to analyze the correlation between Steam market trends (pricing, player counts) and Twitch viewership. It transforms raw, heterogeneous data into a structured warehouse to extract market insights.

## Background & Origin

This project originated from a semester-long group project during my 2nd year of a Bachelor of Technology in Computer Science, specializing in Data Administration, Management, and Analytics.

### The Legacy Core (/legacy)

The /legacy directory contains the original codebase.

- Role: It serves as the primary extraction layer (Scraping & API calls).

- Function: Handles data collection from Steam API, SteamDB, and TwitchTracker.

### Why the Refactor?

While the original project successfully extracted a "gold mine" of raw data, the initial implementation focused primarily on the acquisition phase. I am now re-engineering the entire architecture to move from a script-based approach to a professional data engineering pipeline

### The refactor focuses on:

- Shift from Code to Data: Move away from pure scripting to a robust ELT architecture.

- Ensure Scalability: Replace flat-file management with a proper Data Warehouse.

- Professionalize the Stack: Implement industry-standard tools for transformation and data modeling.

## Tech Stack

- Storage & Warehouse: DuckDB (In-process OLAP database for fast analytical queries).

- Language: Python

- Orchestration: (Planned) Python-based ingestion scripts.

- Transformation: (Planned) dbt (Data Build Tool).

## Getting Started

TODO



_This project is licensed under the MIT License_