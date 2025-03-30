import qrcode
import logging

a  = logging.Handler()
a.emit()



# Link do Google Drive
link_drive = "https://drive.google.com/file/d/13IB73uXf8VdcL2MddnIjp_41DKqyPE2E/view?usp=drive_link"

# Gerar o QR code
qr = qrcode.QRCode(
    version=1,
    error_correction=qrcode.constants.ERROR_CORRECT_L,
    box_size=10,
    border=4,
)
qr.add_data(link_drive)
qr.make(fit=True)

# Criar uma imagem do QR code
img = qr.make_image(fill='black', back_color='white')

# Salvar a imagem
img.save("qrcode_google_drive.png")

# Exibir a imagem
img.show()
