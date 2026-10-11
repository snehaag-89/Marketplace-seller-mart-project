# Marketplace Seller Mart

Data engineering pipeline to model sellers, products, orders, and returns into analytics-ready tables in SQLite, with an AI-generated weekly summary.

## Setup Instructions (Windows PowerShell)

1. **Clone the repository and navigate to it:**
   ```powershell
   cd marketplace-seller-mart
   ```

2. **Create and activate a virtual environment:**
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```

3. **Install dependencies:**
   ```powershell
   pip install -r requirements.txt
   ```

4. **Environment Variables:**
   Copy `.env.example` to `.env` and fill in any required values (e.g., AI provider keys).
   ```powershell
   Copy-Item .env.example .env
   ```

5. **Run Tests:**
   ```powershell
   pytest
   ```
