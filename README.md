# db-acs

## Instructions:

1. go to https://api.census.gov/data/key_signup.html to sign up for an API key and set environmental variable `API_KEY`
2. Run the scripts in order

    ```bash
    python3 01_download.py
    python3 02_nta.py
    python3 03_calculattion.py
    python3 04_special_calculation.py
    python3 05_pivot.py
    ```

3. The final outputs will be saved as the following:

    ```bash
    demo_final_pivoted.csv  
    econ_final_pivoted.csv  
    hous_final_pivoted.csv  
    soci_final_pivoted.csv
    ```
