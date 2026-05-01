@echo off
echo.
echo =============================================
echo   SteelPulse - Procurement Intelligence
echo   Starting Streamlit App...
echo =============================================
echo.

:: Install dependencies if needed
echo [1/2] Checking dependencies...
pip install -r requirements.txt -q

:: Run Streamlit
echo [2/2] Launching app...
echo.
echo   Open your browser at: http://localhost:8501
echo.
streamlit run steelpulse.py --server.port 8501 --server.headless false

pause
