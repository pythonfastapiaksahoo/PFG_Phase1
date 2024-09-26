import base64
import io
import json

from dateutil import parser
from openai import AzureOpenAI
from pdf2image import convert_from_bytes


def stamnpDataFn(blob_data, prompt, deployment_name, api_base, api_key, api_version):

    client = AzureOpenAI(
        api_key=api_key,
        api_version=api_version,
        base_url=f"{api_base}openai/deployments/{deployment_name}",
    )
    pdf_img = convert_from_bytes(blob_data)
    buffered = io.BytesIO()
    pdf_img[0].save(buffered, format="PNG")
    encoded_image = base64.b64encode(buffered.getvalue()).decode("ascii")
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{encoded_image}"},
                },
            ],
        }
    ]

    completion = client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        # max_tokens=1000,
    )

    content = completion.choices[0].message.content
    cl_data = (
        content.replace("json", "")
        .replace("\n", "")
        .replace("'''", "")
        .replace("```", "")
    )
    try:
        stampData = json.loads(cl_data)
    except BaseException:
        try:
            cl_data_corrected = cl_data.replace("'", '"')
            stampData = json.loads(cl_data_corrected)
        except BaseException:
            stampData = cl_data
    # print(stampData)
    return stampData


def VndMatchFn(vndMth_prompt, client):
    vndMth_ck = 0
    vndMth_address_ck = 0

    messages = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": vndMth_prompt},
            ],
        }
    ]

    completion = client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
    )

    content = completion.choices[0].message.content
    cl_mtch = (
        content.replace("json", "")
        .replace("\n", "")
        .replace("'''", "")
        .replace("```", "")
    )
    try:
        vndMth = json.loads(cl_mtch)
        if vndMth["vendorMatching"] == "yes":
            vndMth_ck = 1
        if vndMth["addressMatching"] == "yes":
            vndMth_address_ck = 1
    except BaseException:
        try:
            cl_data_corrected = cl_mtch.replace("'", '"')
            vndMth = json.loads(cl_data_corrected)
            if vndMth["vendorMatching"] == "yes":
                vndMth_ck = 1
            if vndMth["addressMatching"] == "yes":
                vndMth_address_ck = 1

        except BaseException:
            vndMth = cl_mtch
            if '"vendorMatching": "yes"' in content:
                vndMth_ck = 1
            if '"addressMatching": "yes"' in content:
                vndMth_address_ck = 1

    return vndMth_ck, vndMth_address_ck


def is_valid_date(date_string):
    try:
        parser.parse(date_string)
        return True
    except (ValueError, OverflowError):
        return False
