from datetime import date
from cep import Transferencia
from datetime import datetime


today = date.today()
fecha_str = '2024-03-09'
# Convert the string to a date object
fecha = datetime.strptime(fecha_str, '%Y-%m-%d').date()

print("Validating transfer...")
tr = Transferencia.validar(
    fecha=fecha,
    clave_rastreo='MBAN01002403110087494802',
    emisor='40012',  # BBVA
    receptor='90646',  # STP
    cuenta='646180146006124571',
    monto=28500.00,
)

if tr is not None:
    print("Validation successful, downloading PDF...")
    pdf = tr.descargar()
    
    # Define the full path where the PDF will be saved
    file_path = r"C:\Users\p7016\Downloads\transfer.pdf"
    
    # Save the PDF to the specified file
    with open(file_path, 'wb') as f:
        f.write(pdf)
    print(f"PDF saved successfully at {file_path}.")
else:
    print("Validation failed, unable to download PDF.")
