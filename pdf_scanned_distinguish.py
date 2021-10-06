"""
Extract images from a PDF file, confirm number of images found.
"""
import os
import fitz
import ocrmypdf
import pytesseract
from PIL import Image

scriptdir = os.path.abspath(os.path.dirname(__file__))
filename = os.path.join(scriptdir, "resources", "joined.pdf")
known_image_count = 21


def extract_image_pdf():
    doc = fitz.open(r"/Users/mlamp/Documents/文档原件/扫描/51036.pdf")

    image_count = 1
    for xref in range(1, doc.xref_length() - 1):
        if doc.xref_get_key(xref, "Subtype")[1] != "/Image":
            continue
        img = doc.extract_image(xref)
        if isinstance(img, dict):
            image_count += 1
    print(image_count)
    # assert image_count == known_image_count  # this number is know about the file

def extract_image(image):
    image = Image.open('test.png')
    code = pytesseract.image_to_string(image)
    print(code)


if __name__ == '__main__':
    extract_image_pdf()