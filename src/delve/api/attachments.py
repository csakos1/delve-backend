"""REST for attachments (B6).

`POST /attachments?kind=image|file&filename=…` takes the raw bytes as the body, with the
type in `Content-Type` (no multipart: one file per request, no extra dependency).
`chat.send` then binds the returned ids. `GET /attachments/{id}` serves the bytes to the
app; `POST /attachments/{id}/copy` makes a new upload of an earlier one ("send again").
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any, Literal
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse

from delve.api.rest import Device
from delve.attachments import MAX_FILE_BYTES, MAX_IMAGE_BYTES, AttachmentError, AttachmentStore

router = APIRouter(prefix="/api/v1")


def _store(request: Request) -> AttachmentStore:
    store: AttachmentStore = request.app.state.attachments
    return store


def _http(exc: AttachmentError) -> HTTPException:
    return HTTPException(exc.status, {"code": exc.code, "message": exc.message})


async def _body(request: Request, limit: int) -> bytes:
    """The request body, refused as soon as it passes `limit` bytes."""
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > limit:
        raise _http(AttachmentError(413, "too_large", f"the upload may be {limit} bytes"))
    chunks, size = [], 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            raise _http(AttachmentError(413, "too_large", f"the upload may be {limit} bytes"))
        chunks.append(chunk)
    return b"".join(chunks)


@router.post("/attachments", status_code=201)
async def upload(
    request: Request,
    _device: Device,
    kind: Annotated[Literal["image", "file"], Query()],
    filename: Annotated[str | None, Query(max_length=1024)] = None,
) -> dict[str, Any]:
    data = await _body(request, MAX_IMAGE_BYTES if kind == "image" else MAX_FILE_BYTES)
    try:
        return await _store(request).save(
            kind=kind, mime=request.headers.get("content-type"), filename=filename, data=data
        )
    except AttachmentError as exc:
        raise _http(exc) from exc


@router.get("/attachments/{attachment_id}")
async def download(request: Request, _device: Device, attachment_id: uuid.UUID) -> FileResponse:
    try:
        a, path = await _store(request).locate(attachment_id)
    except AttachmentError as exc:
        raise _http(exc) from exc
    if not path.is_file():
        raise _http(AttachmentError(410, "attachment_gone", "the attachment was deleted"))
    disposition = "inline" if a.kind == "image" else "attachment"
    return FileResponse(
        path,
        media_type=a.mime,
        headers={
            "Content-Disposition": f"{disposition}; filename*=UTF-8''{quote(a.filename)}",
            "Cache-Control": "private, max-age=86400, immutable",
            "ETag": f'"{a.sha256}"',
        },
    )


@router.post("/attachments/{attachment_id}/copy", status_code=201)
async def copy(request: Request, _device: Device, attachment_id: uuid.UUID) -> dict[str, Any]:
    try:
        return await _store(request).copy(attachment_id)
    except AttachmentError as exc:
        raise _http(exc) from exc
