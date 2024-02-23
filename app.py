import streamlit as st
import boto3
import json
import requests
import os
from typing import List
from PyPDF2 import PdfReader, PdfWriter


"# Long Document Uploader"

reducto_api_key = st.text_input("Reducto API Key", type="password")
document_url = st.text_input("Document URL")
chunk_size = st.text_input("Chunk Size", "500")

if document_url:
    # Function to split PDF into segments of 500 pages each
    def split_pdf(document_url: str, segment_size: int = 500):
        # Download the PDF
        response = requests.get(document_url)
        original_pdf_path = "original_document.pdf"
        with open(original_pdf_path, "wb") as f:
            f.write(response.content)

        # Read the PDF
        reader = PdfReader(original_pdf_path)
        total_pages = len(reader.pages)

        # Calculate the number of segments needed
        num_segments = total_pages // segment_size + (
            1 if total_pages % segment_size else 0
        )

        segment_paths: List[str] = []
        for segment in range(num_segments):
            writer = PdfWriter()
            start_page = segment * segment_size
            end_page = min(start_page + segment_size, total_pages)

            # Add pages to each segment
            for page_num in range(start_page, end_page):
                writer.add_page(reader.pages[page_num])

            # Save each segment to a new file
            segment_path = f"segment_{segment + 1}.pdf"
            with open(segment_path, "wb") as segment_file:
                writer.write(segment_file)

            segment_paths.append(segment_path)

        # Clean up the original downloaded PDF
        os.remove(original_pdf_path)

        return segment_paths

    # Split the PDF and get paths of the segments
    segment_paths = split_pdf(document_url)
    print(f"PDF split into {len(segment_paths)} segments: {segment_paths}")

    s3 = boto3.client(
        "s3",
        region_name="us-west-2",
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
    )

    progress_bar = st.progress(0)
    progress_text = st.empty()
    for i, segment in enumerate(segment_paths):
        progress_text.text(f"Processing segment {i+1} of {len(segment_paths)}")
        progress_bar.progress((i + 1) / len(segment_paths))
        s3.upload_file(segment, st.secrets["AWS_S3_BUCKET"], segment)
        document_part_url = s3.generate_presigned_url(
            "get_object", Params={"Bucket": "AWS_S3_BUCKET", "Key": segment}
        )

        url = "https://api.reducto.ai/chunk_url"
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {reducto_api_key}",
        }
        st.write(f"running {document_part_url}")
        params = {"document_url": document_part_url, "chunk_size": chunk_size}
        response = requests.post(url, headers=headers, params=params)

        json_download_url = response.json()

        with open(segment.replace(".pdf", ".json"), "w") as f:
            if isinstance(json_download_url, str):
                content = requests.get(json_download_url).content.decode("utf-8")
            else:
                content = json.dumps(json_download_url)
            f.write(content)

    st.write("Concatenating chunks...")

    full_output = []

    for i, segment in enumerate(segment_paths):
        offset = i * 500
        with open(segment.replace(".pdf", ".json"), "r") as f:
            segment_json = json.loads(f.read())
        st.write(segment_json)
        for chunk in segment_json:
            chunk["metadata"]["page"] += offset
            for bbox in chunk["metadata"]["bbox"]:
                bbox["page"] += offset

        full_output.extend(segment_json)

    with open("full_output.json", "w") as f:
        json.dump(full_output, f)

    st.success("Processing complete!")

    download_button = st.download_button(
        label="Download JSON Output",
        data=json.dumps(full_output),
        file_name="full_output.json",
        mime="application/json",
    )
