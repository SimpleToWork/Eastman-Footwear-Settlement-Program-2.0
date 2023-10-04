@echo off

cd /d "C:\Users\%USERNAME%\Desktop\New Projects\Eastman Footwear\Eastman-Footwear-Settlement-Program"
python -m virtualenv venv

cd venv/scripts
call activate.bat

cd /d "C:\Users\%USERNAME%\Desktop\New Projects\Eastman Footwear\Eastman-Footwear-Settlement-Program"
pip install -r requirements.txt

cmd /kcd