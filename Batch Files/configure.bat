@echo off

cd /d "C:\Users\%USERNAME%\Desktop\New Projects\Eastman Footwear\Eastman-Footwear-Settlement-Program-2.0"
python -m virtualenv venv

cd venv/scripts
call activate.bat

cd /d "C:\Users\%USERNAME%\Desktop\New Projects\Eastman Footwear\Eastman-Footwear-Settlement-Program-2.0"
pip install -r requirements.txt

cmd /kcd