import math
import sys
from collections import defaultdict
from io import BytesIO

import cv2
import numpy as np
from PIL import Image
from pypdf import PdfReader, PdfWriter

accepted_inch = 5 * 72
accepted_pixel_max = 8000
accepted_pixel_min = 50
accepted_filesize_max = 50
# fields = {}
# loc = {}
# active_field = ""


def rotate(point, origin, degrees):
    try:
        radians = np.deg2rad(degrees)
        x, y = point
        offset_x, offset_y = origin
        adjusted_x = x - offset_x
        adjusted_y = y - offset_y
        cos_rad = np.cos(radians)
        sin_rad = np.sin(radians)
        qx = offset_x + cos_rad * adjusted_x + sin_rad * adjusted_y
        qy = offset_y + -sin_rad * adjusted_x + cos_rad * adjusted_y
        return qx, qy
    except Exception as e:
        print(f"exe {e}")
        return 0, 0


def correctAngle(analysis):
    for page in analysis["analyzeResult"]["readResults"]:
        if page["angle"] != 0:
            for line in page["lines"]:
                bBox = line["boundingBox"]
                for ind in range(0, 7, 2):
                    bBox[ind], bBox[ind + 1] = rotate(
                        (bBox[ind], bBox[ind + 1]), (0, 0), page["angle"]
                    )
                line["boundingBox"] = bBox
                for word in line["words"]:
                    wbBox = word["boundingBox"]
                    for ind in range(0, 7, 2):
                        wbBox[ind], wbBox[ind + 1] = rotate(
                            (wbBox[ind], wbBox[ind + 1]), (0, 0), page["angle"]
                        )
                    word["boundingBox"] = wbBox
    return analysis


def rotate_custom(image, angle, center=None, scale=1.0):
    try:
        (h, w) = image.shape[:2]

        if center is None:
            center = (w / 2, h / 2)

        # Perform the rotation
        M = cv2.getRotationMatrix2D(center, angle, scale)
        rotated = cv2.warpAffine(image, M, (w, h))

        return rotated
    except Exception as e:
        return image


async def get_file(file, w):
    try:
        content = await file.read()
        if file.content_type != "application/pdf":
            img = Image.open(BytesIO(content))
            width, height = img.size
            if width > w:
                height = w * (height / width)
                img.thumbnail((w, int(height)), Image.ANTIALIAS)
                byte_io = BytesIO()
                format = "PNG"
                if file.content_type == "image/jpg":
                    format = "JPG"
                elif file.content_type == "image/jpeg":
                    format = "JPEG"
                else:
                    format = "PNG"
                img.save(byte_io, format)
                content = byte_io.getvalue()
        return ((file.content_type, file.filename), BytesIO(content), True)
        # status,data = preprocess(file.filename,BytesIO(content),file.content_type)
        # if status:
        #     return ((file.content_type, file.filename), data, True)
        # else:
        #     return ((file.content_type, file.filename), b"",False)
    except Exception as e:
        print(f"Exception at util.py {str(e)}")
        return "", b"", False


def preprocess(file_name, file_bytes, file_type):
    try:

        global accepted_inch, accepted_pixel_max, accepted_pixel_min, accepted_filesize_max
        if file_type == "application/pdf":
            pdf = PdfReader(file_bytes, strict=False)
            if not pdf.isEncrypted:
                dimention = pdf.getPage(0).mediaBox
                writer = PdfWriter()
                num_pages = pdf.getNumPages()
                for page_no in range(num_pages):
                    page = pdf.getPage(page_no)
                    if max(dimention[2], dimention[3]) > accepted_inch:
                        print(f"Resizing Pdf {file_name} - Page {page_no+1}")
                        page.scaleBy(
                            accepted_inch / max(int(dimention[2]), int(dimention[3]))
                        )
                    writer.addPage(page)

                tmp = BytesIO()
                writer.write(tmp)
                data = tmp.getvalue()
            else:
                return False, "File is Encrypted"
        else:
            img = Image.open(file_bytes)
            w, h = img.size
            if w <= accepted_pixel_min or h <= accepted_pixel_min:
                # Discard this due to low quality
                print(f"Discard {file_name} due to low quality.")
                return False, f"File is below {accepted_pixel_min}"
            elif w >= accepted_pixel_max or h >= accepted_pixel_max:
                """# resize image"""
                print(f"Resize the Image. {file_name}")
                factor = accepted_pixel_max / max(img.size[0], img.size[1])
                img.thumbnail(
                    (int(img.size[0] * factor), int(img.size[1] * factor)),
                    Image.ANTIALIAS,
                )

            byte_io = BytesIO()
            format = "PNG" if file_type == "png" else "JPEG"
            img.save(byte_io, format)
            data = byte_io.getvalue()
        # Upload to Blob

        """ If Filesize is greater than prescribed reject"""
        if math.ceil(sys.getsizeof(data) / 1024 / 1024) >= accepted_filesize_max:
            print(f"Filesize for {file_name} is more..")
            return False, f"File Size is above {accepted_filesize_max}"
        return True, data
    except Exception as e:
        print(f"Exception in util.py {str(e)}")
        # exc_type, exc_obj, exc_tb = sys.exc_info()
        # fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        # logging.info(exc_type, fname, exc_tb.tb_lineno, str(e))
        return False, b""


def nested_dict():
    """Creates a default dictionary where each value is an other default
    dictionary."""
    return defaultdict(nested_dict)


def default_to_regular(d):
    """Converts defaultdicts of defaultdicts to dict of dicts."""
    if isinstance(d, defaultdict):
        d = {k: default_to_regular(v) for k, v in d.items()}
    return d


def get_path_dict(paths):
    new_path_dict = nested_dict()
    for path in paths:
        parts = path.split("/")
        if parts:
            marcher = new_path_dict
            for key in parts[:-1]:
                marcher = marcher[key]
            marcher[parts[-1]] = parts[-1]
    return default_to_regular(new_path_dict)
