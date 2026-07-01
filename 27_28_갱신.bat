@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ============================================
echo   27/28 컨센서스 갱신 (FnGuide, 한국 IP 전용)
echo ============================================
echo.
echo [1/2] 최신 코드/데이터 동기화 (git pull)...
git pull --rebase --autostash origin main
echo.
echo [2/2] FnGuide 27/28E 수집 시작...
echo.
python refresh_27_28.py
echo.
echo ============================================
echo   완료. 아무 키나 누르면 창이 닫힙니다.
echo ============================================
pause >nul
