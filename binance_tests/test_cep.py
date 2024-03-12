from datetime import date
from cep import Transferencia

today = date.today()

print("Validating transfer...")
tr = Transferencia.validar(
    fecha=today,
    clave_rastreo='085904246950305946',
    emisor='40072',  # Banorte
    receptor='90646',  # STP
    cuenta='646180204200033494',
    monto=50000.00,
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
