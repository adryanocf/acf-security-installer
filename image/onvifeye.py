#!/usr/bin/python3
"""
onvifeye: ONVIF event monitor and clip recorder
===============================================

Usage:
======

        onvifeye [--verbose] [--create camera-config-file.conf]
                  [--help | --detailed-help]

Optional arguments:
-------------------

      -h, --help            show this help message and exit
      --detailed-help       full help in Markdown format

Description
===========

Monitor for camera ONVIF events and record them using RSTP.
Recording is performed by ffmpeg.

Currently only tested against a TP-Link Tapo-C225.  With slight modifications
this script will likely work with other cameras.

Run initially by supplying command line parameters to create a config file.

Config files
------------

$HOME/.config/onvifeye/onvifeye default camera config


Prerequisites
=============

* python3
* onvif-zeep-async (pip install onvif-zeep-async)
* ffmpeg-python (pip install ffmpeg-python). Take care not to confuse
  ffmpeg-python with python-ffmpeg the two are different ffmpeg python
  implementations.

onvifeye Copyright (C) 2025 Michael Hamilton
=============================================

This program is free software: you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see <https://www.gnu.org/licenses/>.

**Contact:**  m i c h a e l   @   a c t r i x   .   g e n   .   n z

----------
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import logging
import os
import signal
import subprocess
import sys
import termios
import time
import traceback
from abc import ABC, abstractmethod
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path
from subprocess import Popen
from typing import Dict, Callable

MAX_FAIL_TO_POST_ERRORS = 5

WAIT_NO_NOTIFICATIONS_SECONDS = 0.1

WAIT_NO_NOTIFICATIONS_EXCEPTION_SECONDS = 5.0

DEFAULT_DETECTION_EXPIRY_SECONDS = 60.0

WAIT_FOR_LOCAL_VIDEO_SECONDS = 5.0

EVENT_NOT_HAPPENING_SUFFIX = '_False'

VIDEO_ENDED_SYNTHETIC_EVENT = 'VideoEnded'

WILDCARD_EVENT = '*'

try_ws_discovery = False
if try_ws_discovery:
    from wsdiscovery import Scope, QName
    from wsdiscovery.discovery import ThreadedWSDiscovery as WSDiscovery

import ffmpeg
import httpx
from onvif import ONVIFCamera, ONVIFService, ONVIFError
from importlib import resources as imp_resources
from urllib.parse import urlparse, quote
from onvif.managers import PullPointManager
import asyncio
import aiohttp

log = logging.getLogger('onvifeye')
#logging.getLogger('onvif').setLevel(logging.DEBUG)

os.environ['OPENCV_FFMPEG_LOGLEVEL'] = '8'
logging.getLogger("httpx").setLevel(logging.CRITICAL)

EXCEPTION_RETRY_WAIT_SECONDS = 5

CAMERA_ONVIF_WSDL_DIR = imp_resources.files('onvif') / 'wsdl'
log.info(f"{CAMERA_ONVIF_WSDL_DIR=}")

CONFIG_DIR = Path.home() / '.config' / 'onvifeye'
CAMERA_CONFIG_DIR = CONFIG_DIR / Path('camera_conf')
DATA_DIR = Path.home() / 'onvifeye'
VIDEO_DIR: Path = DATA_DIR / Path('videos')
IMAGE_DIR = DATA_DIR / Path('images')


class CameraConfig(object):
    def __init__(self,
                 camera_username = 'tapo-admin',
                 camera_password = '',
                 camera_id = '',
                 camera_model = '',
                 camera_ip_addr = '',
                 camera_onvif_port = '2020',
                 camera_stream_name = 'mainStream',
                 camera_stills_stream_name = 'jpegStream',
                 camera_clip_seconds = 30,
                 camera_target_events = ('IsPeople', 'IsCar'),
                 camera_event_exec = '',
                 camera_event_exec_delay_seconds=0,
                 camera_save_folder = DATA_DIR.as_posix(),
                 camera_grab_stills_from_video = True):
        super().__init__()
        self.camera_username = camera_username
        self.camera_password = camera_password
        self.camera_id = camera_id if camera_id else camera_ip_addr
        self.camera_model = camera_model  # for future use
        self.camera_ip_addr = camera_ip_addr
        self.camera_onvif_port = camera_onvif_port
        self.camera_target_events = camera_target_events
        self.camera_stream_name = camera_stream_name
        self.camera_stills_stream_name = camera_stills_stream_name
        self.camera_clip_seconds = camera_clip_seconds
        self.camera_event_exec = camera_event_exec
        self.camera_event_exec_delay_seconds = camera_event_exec_delay_seconds
        self.camera_save_folder = camera_save_folder
        self.camera_grab_stills_from_video = camera_grab_stills_from_video

    def is_event_targeted(self, event_name: str) -> bool:
        return self.camera_target_events == '*' or event_name in self.camera_target_events

# https://stackoverflow.com/a/75060902/609575
def uri_add_authentication(url, username, password):
    username = quote(username)
    password = quote(password)
    url = urlparse(url)
    netloc = url.netloc.split('@')[-1]
    url = url._replace(netloc=f'{username}:{password}@{netloc}')
    return url.geturl()


class TargetCamera:

    def __init__(self, camera_config: CameraConfig):
        super().__init__()
        self.config = camera_config
        self.onvif = None
        self.detections: Dict[str, datetime] = {}
        self.presence_state = {}
        self.presence_events = []
        self.presence_pending = {}


class NotificationPuller:

    def __init__(self, target_camera: TargetCamera):
        self.target_camera = target_camera
        self.camera_id = self.target_camera.config.camera_id
        self.pullpoint_manager: PullPointManager | None  = None
        self.pullpoint_service: ONVIFService | None = None
        self.stop_requested = False
        self.log_name = f"{self.__class__.__name__}: {self.camera_id}"
        # Not sure what detection_expiry_seconds should be:
        self.detection_expiry_seconds = DEFAULT_DETECTION_EXPIRY_SECONDS

    async def connect(self):
        attempt_count = 1
        while self.pullpoint_service is None:
            try:
                if attempt_count == 1:
                    log.info(F"{self.log_name} connecting.")
                camera_config = self.target_camera.config
                self.target_camera.onvif = ONVIFCamera(
                    camera_config.camera_ip_addr,
                    int(camera_config.camera_onvif_port),
                    camera_config.camera_username,
                    camera_config.camera_password,
                    str(CAMERA_ONVIF_WSDL_DIR))
                await self.target_camera.onvif.update_xaddrs()
                interval_time = (timedelta(seconds=self.detection_expiry_seconds))
                self.pullpoint_manager = await self.target_camera.onvif.create_pullpoint_manager(
                    interval_time,
                    subscription_lost_callback=self.subscription_lost)
                pullpoint_address = self.target_camera.onvif.xaddrs.get(
                    "http://www.onvif.org/ver10/events/wsdl/PullPointSubscription"
                )
                log.info(f'{self.log_name} PullPoint subscription address: {pullpoint_address}')
                self.pullpoint_service = self.pullpoint_manager.get_service()
                log.info(F"{self.log_name} connected on {attempt_count=}")
            except (httpx.HTTPError, Exception) as httpx_exception:
                if attempt_count == 1:
                    log.warning(f'{self.log_name} connect: http error, retrying'
                                f' every {EXCEPTION_RETRY_WAIT_SECONDS} seconds. [{repr(httpx_exception)}]')
                elif log.isEnabledFor(logging.DEBUG):
                    log.debug(f'{self.log_name} {attempt_count=} connect: http error, retrying'
                                f' every {EXCEPTION_RETRY_WAIT_SECONDS} seconds. [{repr(httpx_exception)}]')
                attempt_count += 1
                await asyncio.sleep(EXCEPTION_RETRY_WAIT_SECONDS)

    def subscription_lost(self):
        log.info(F"{self.log_name} subscription_lost called.")
        # Not required because the listen() loops and reconnects on Exceptions?
        pass

    async def listen(self):
        while not self.stop_requested:
            try:
                if self.pullpoint_service is None:
                    await self.connect()
                pullpoint_req = self.pullpoint_service.create_type('PullMessages')
                pullpoint_req.MessageLimit = 5000
                pullpoint_req.Timeout = (timedelta(days=0, hours=0,
                                                   seconds=self.detection_expiry_seconds))
                log.info(F"{self.log_name} listening, pulling messages ...")
                while not self.stop_requested:
                    try:
                        # throws httpx.RemoteProtocolError if it times out
                        camera_messages = await self.pullpoint_service.PullMessages(pullpoint_req)
                        notifications = (
                            camera_messages['NotificationMessage']
                            if camera_messages else None
                        )
                        if notifications:
                            log.info(
                                f'{self.log_name} PullMessages received '
                                f'{len(notifications)} notification(s)'
                            )
                        else:
                            log.debug(
                                f'{self.log_name} PullMessages returned no notifications'
                            )
                        if notifications:
                            for notification_msg in camera_messages['NotificationMessage']:
                                if log.isEnabledFor(logging.DEBUG):  # Avoid expensive debugging
                                    log.debug(f"{self.log_name} {notification_msg=}")
                                event_message = notification_msg['Message']['_value_1']
                                property_operation = event_message['PropertyOperation']
                                data = event_message['Data']
                                for simple_item in data['SimpleItem']:
                                    if simple_item['Name'] == 'IsPeople':
                                        log.info(
                                            '%s DIAG IsPeople value=%s operation=%s utc=%s received_utc=%s',
                                            self.log_name,
                                            simple_item['Value'],
                                            event_message['PropertyOperation'],
                                            event_message['UtcTime'],
                                            datetime.now(timezone.utc).isoformat(),
                                        )
                                    base_detection = simple_item['Name']
                                    type_of_detection = base_detection
                                    is_happening = simple_item['Value'] == 'true'
                                    previous_state = self.target_camera.presence_state.get(base_detection)
                                    event_time = datetime.now()
                                    new_people_presence = False

                                    if base_detection == 'IsPeople':
                                        pending = self.target_camera.presence_pending.get(base_detection)
                                        if pending is not None and pending[0] == is_happening:
                                            count = pending[1] + 1
                                            first_seen = pending[2]
                                        else:
                                            count = 1
                                            first_seen = event_time

                                        self.target_camera.presence_pending[base_detection] = (
                                            is_happening, count, first_seen
                                        )

                                        transition_changed = (
                                            previous_state is None
                                            or previous_state[0] != is_happening
                                        )
                                        stable_presence = (
                                            count >= 3
                                            and (event_time - first_seen).total_seconds() >= 2
                                        )
                                        # Confirm absence immediately; handle_events applies
                                        # the existing 10-second absence debounce.
                                        if transition_changed and (
                                            not is_happening or stable_presence
                                        ):
                                            self.target_camera.presence_state[base_detection] = (
                                                is_happening, event_time
                                            )
                                            self.target_camera.presence_events.append(
                                                (is_happening, event_time)
                                            )
                                            new_people_presence = is_happening
                                            log.info(
                                                f'{self.log_name} confirmed IsPeople transition '
                                                f'after {count} consecutive notifications: '
                                                f'{is_happening=} {event_time=}'
                                            )
                                    else:
                                        self.target_camera.presence_state[base_detection] = (
                                            is_happening, event_time
                                        )
                                    if not is_happening:
                                        type_of_detection += EVENT_NOT_HAPPENING_SUFFIX
                                    if type_of_detection not in self.target_camera.detections or new_people_presence:
                                        self.target_camera.detections[type_of_detection] = event_time
                                        log.info(f'{self.log_name} received {type_of_detection} event, added it to {self.target_camera.detections=}')
                        else:
                            await asyncio.sleep(WAIT_NO_NOTIFICATIONS_SECONDS)
                    except (aiohttp.ServerDisconnectedError, httpx.RemoteProtocolError, httpx.ReadTimeout) as nothing_ready:
                        # These exceptions appear to occur if there is nothing available, but, curiously,
                        # they can occur more frequently than self.detection_expiry_seconds
                        # onvif-zeep-async 3.2.5 throws httpx.RemoteProtocolError
                        # onvif-zeep-async 4.0.4 throws aiohttp.ServerDisconnectedError (every ~10 seconds).
                        if log.isEnabledFor(logging.DEBUG):
                            log.debug(f'{self.log_name} No messages ready [{repr(nothing_ready)}]')
                        await asyncio.sleep(WAIT_NO_NOTIFICATIONS_EXCEPTION_SECONDS)  # Don't flood the camera with requests
                    finally:
                        now = datetime.now()
                        for type_of_detection, first_seen_at in [
                            (name, first_seen_at)
                            for name, first_seen_at in self.target_camera.detections.items()
                            if (name != "IsPeople" or (self.target_camera.presence_state.get("IsPeople") is not None and not self.target_camera.presence_state["IsPeople"][0])) and (now - first_seen_at).total_seconds() > self.detection_expiry_seconds]:
                                del self.target_camera.detections[type_of_detection]
                                log.info(f"{self.log_name} expire '{type_of_detection}': {first_seen_at} -> {self.target_camera.detections=}")
            except Exception as listen_exception:
                log.warning(f'{self.log_name} listen: unexpected exception. {listen_exception.__class__.__module__}: [{repr(listen_exception)}]')
            finally:
                try:
                    await self.disconnect()
                except Exception as disconnect_exception:
                    log.warning(f'{self.log_name} listen: ignoring disconnect exception. {disconnect_exception.__class__.__module__}: [{repr(disconnect_exception)}]')
                self.pullpoint_service = None
                if not self.stop_requested:
                    log.warning(f'{self.log_name} listen: attempting reconnect in {EXCEPTION_RETRY_WAIT_SECONDS} seconds')
                    await asyncio.sleep(EXCEPTION_RETRY_WAIT_SECONDS)

    async def disconnect(self):
        errors = []
        try:
            if self.pullpoint_service:
                try:
                    await self.pullpoint_service.close()
                except Exception as sce:
                    errors.append(f"pullpoint_service.close: {sce}")
            if self.pullpoint_manager:
                try:
                    await self.pullpoint_manager.shutdown()
                except Exception as mse:
                    errors.append(f"pullpoint_manager.shutdown: {mse}")
            if self.target_camera.onvif:
                try:
                    await self.target_camera.onvif.close()
                except Exception as oce:
                    errors.append(f"target_camera.onvif.close: {oce}")
        finally:
            self.pullpoint_service = None
            self.pullpoint_manager = None
            self.target_camera.onvif = None
        if errors:
            raise Exception("Disconnection errors: " + "; ".join(errors))



def event_media_id(camera_id: str, detection: str, incident_time: datetime) -> str:
    event_time = incident_time.strftime("%Y%m%d-%H%M%S")
    event_key = f"{camera_id}|{detection}|{event_time}"
    return "onvif-" + hashlib.sha256(event_key.encode()).hexdigest()[:40]


def publish_video_ready(camera_id: str, detection: str,
                        incident_time: datetime, video_path: Path,
                        video_started_at: datetime):
    if not video_path.is_file() or video_path.stat().st_size == 0:
        raise ValueError(f"Vídeo ausente ou vazio: {video_path}")

    event_id = event_media_id(camera_id, detection, incident_time)
    pending_dir = DATA_DIR / "media-index" / camera_id / ".pending"
    pending_dir.mkdir(parents=True, exist_ok=True)
    marker = pending_dir / f"{event_id}.json"
    temp = marker.with_name(f"{marker.name}.{os.getpid()}.tmp")
    record = {
        "event_id": event_id,
        "camera_id": camera_id,
        "event_type": detection,
        "event_time": incident_time.strftime("%Y%m%d-%H%M%S"),
        "video_path": video_path.relative_to(DATA_DIR).as_posix(),
        "video_started_at": video_started_at.isoformat(),
    }
    try:
        temp.write_text(json.dumps(record, ensure_ascii=False) + "\n",
                        encoding="utf-8")
        os.replace(temp, marker)
    finally:
        if temp.exists():
            temp.unlink()


def write_media_index(camera_id: str, detection: str, incident_time: datetime,
                      video_path: Path, image_path: Path,
                      video_started_at: datetime, offset_seconds: float):
    for media_path in (video_path, image_path):
        if not media_path.is_file() or media_path.stat().st_size == 0:
            raise ValueError(f"Mídia ausente ou vazia: {media_path}")

    event_id = event_media_id(camera_id, detection, incident_time)
    index_dir = DATA_DIR / "media-index" / camera_id
    index_dir.mkdir(parents=True, exist_ok=True)
    index_path = index_dir / f"{event_id}.json"
    temp = index_path.with_name(f"{index_path.name}.{os.getpid()}.tmp")

    actual_frame_time = video_started_at + timedelta(seconds=offset_seconds)
    record = {
        "schema_version": 1,
        "event_id": event_id,
        "camera_id": camera_id,
        "event_type": detection,
        "event_time": incident_time.strftime("%Y%m%d-%H%M%S"),
        "occurred_at": incident_time.astimezone().astimezone(
            timezone.utc
        ).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "video_path": video_path.relative_to(DATA_DIR).as_posix(),
        "image_path": image_path.relative_to(DATA_DIR).as_posix(),
        "video_started_at": video_started_at.astimezone().isoformat(),
        "image_offset_seconds": round(offset_seconds, 3),
        "image_frame_time": actual_frame_time.astimezone().isoformat(),
        "created_at": datetime.now().astimezone().isoformat(),
    }
    try:
        temp.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8")
        os.replace(temp, index_path)
        log.info(f"Índice de mídia salvo: {index_path} event_id={event_id}")
    finally:
        if temp.exists():
            temp.unlink()



def save_video(camera_config: CameraConfig, rtsp_uri: str,
               clip_seconds: int, detections: Dict[str, datetime]) -> bool:
    video_started_at = datetime.now()
    save_path = generate_save_path(
        camera_config.camera_id, video_started_at, VIDEO_DIR, "mp4")
    log.info(f"writing {save_path.as_posix()}")

    if save_path.exists():
        log.error(f"Gravação recusada: o caminho já existe, sem associação segura: {save_path}")
        return False

    succeeded = False
    try:
        process = ffmpeg.input(
            rtsp_uri, t=clip_seconds, loglevel=24, rtsp_transport="tcp"
        ).output(
            filename=save_path.as_posix(), f="mpegts",
            vcodec="h264", acodec="aac", preset="ultrafast",
            tune="zerolatency", loglevel=8
        ).run_async(
            pipe_stdout=False, pipe_stderr=True, overwrite_output=True
        )
        try:
            out, err = process.communicate(timeout=clip_seconds + 30)
        except subprocess.TimeoutExpired:
            log.error(f"Timeout do FFmpeg ao gravar {save_path}")
            process.kill()
            process.communicate()
            save_path.unlink(missing_ok=True)
            return False

        log_ffmpeg_output(out, err)
        if process.returncode != 0:
            log.error(f"FFmpeg terminou com código {process.returncode}: {save_path}")
            save_path.unlink(missing_ok=True)
            return False
        if not save_path.is_file() or save_path.stat().st_size == 0:
            log.error(f"FFmpeg não gerou vídeo válido: {save_path}")
            save_path.unlink(missing_ok=True)
            return False

        succeeded = True
        for detection, event_time in detections.items():
            try:
                publish_video_ready(
                    camera_config.camera_id, detection, event_time,
                    save_path, video_started_at
                )
            except Exception:
                succeeded = False
                log.exception(
                    f"Falha ao publicar vídeo para {detection} em {event_time}"
                )
        log.info(f"closed {save_path.as_posix()}")
        return succeeded

    except ffmpeg.Error as exc:
        log.error(f"Erro do FFmpeg ao gravar {save_path}: {exc}")
        log_ffmpeg_output(exc.stdout, exc.stderr, as_error=True)
        save_path.unlink(missing_ok=True)
        return False
    finally:
        if camera_config.is_event_targeted(VIDEO_ENDED_SYNTHETIC_EVENT):
            execute_external_handler(
                Path(camera_config.camera_event_exec),
                camera_config.camera_id,
                {VIDEO_ENDED_SYNTHETIC_EVENT: datetime.now()}
            )


def extract_frame_to_image(camera_config: CameraConfig, incident_time: datetime,
                           image_save_path: Path, detection: str):
    camera_id = camera_config.camera_id
    event_id = event_media_id(camera_id, detection, incident_time)
    marker = DATA_DIR / "media-index" / camera_id / ".pending" / f"{event_id}.json"
    deadline = time.monotonic() + max(
        60, camera_config.camera_clip_seconds + 30
    )

    while not marker.is_file() and time.monotonic() < deadline:
        time.sleep(0.5)

    if not marker.is_file():
        log.error(f"Tempo esgotado aguardando vídeo do evento {event_id}")
        return None, None, None

    try:
        record = json.loads(marker.read_text(encoding="utf-8"))
        video_path = DATA_DIR / record["video_path"]
        video_started_at = datetime.fromisoformat(record["video_started_at"])

        if not video_path.is_file() or video_path.stat().st_size == 0:
            log.error(f"Vídeo associado ausente ou vazio: {video_path}")
            return None, None, None

        offset = max(0.0, (incident_time - video_started_at).total_seconds())
        try:
            probe = ffmpeg.probe(video_path.as_posix())
            duration = float(probe["format"]["duration"])
            offset = min(offset, max(0.0, duration - 0.1))
        except Exception:
            log.warning(f"Não foi possível consultar a duração de {video_path}")

        if not image_save_path.is_file() or image_save_path.stat().st_size == 0:
            out, err = ffmpeg.input(
                video_path.as_posix(), ss=offset, loglevel=8
            ).output(
                image_save_path.as_posix(), vframes=1, qscale=2
            ).run(
                capture_stdout=False, capture_stderr=True,
                overwrite_output=True, quiet=True
            )
            log_ffmpeg_output(out, err)

        if not image_save_path.is_file() or image_save_path.stat().st_size == 0:
            log.error(f"FFmpeg não produziu imagem válida: {image_save_path}")
            return None, None, None

        return video_path, video_started_at, offset

    except Exception:
        log.exception(f"Falha ao extrair imagem para o evento {event_id}")
        return None, None, None



def save_image(camera_config: CameraConfig, rtsp_uri: str,
               detections: Dict[str, datetime],
               grab_stills_from_video: bool) -> bool:
    if not detections:
        return False

    if grab_stills_from_video:
        succeeded = True
        for detection, incident_time in detections.items():
            camera_id = camera_config.camera_id
            image_path = generate_save_path(
                camera_id, incident_time, IMAGE_DIR, "jpg"
            )
            video_path, video_started_at, offset = extract_frame_to_image(
                camera_config, incident_time, image_path, detection
            )
            if not video_path or video_started_at is None or offset is None:
                succeeded = False
                continue

            try:
                write_media_index(
                    camera_id, detection, incident_time,
                    video_path, image_path, video_started_at, offset
                )
                marker = (
                    DATA_DIR / "media-index" / camera_id / ".pending"
                    / f"{event_media_id(camera_id, detection, incident_time)}.json"
                )
                marker.unlink(missing_ok=True)
            except Exception:
                succeeded = False
                log.exception(
                    f"Falha ao indexar {detection} em {incident_time}"
                )
        return succeeded

    time.sleep(0.5)
    camera_id = camera_config.camera_id
    incident_time = next(iter(detections.values()))
    save_path = generate_save_path(camera_id, incident_time, IMAGE_DIR, "jpg")
    if save_path.is_file() and save_path.stat().st_size > 0:
        log.info(f"save_image: imagem já existe: {save_path}")
        return True

    try:
        out, err = ffmpeg.input(
            rtsp_uri, loglevel=8, rtsp_transport="tcp"
        ).output(
            filename=save_path.as_posix(), vframes=1, loglevel=8
        ).run(
            capture_stdout=False, capture_stderr=True,
            overwrite_output=True, quiet=True
        )
        log_ffmpeg_output(out, err)
    except ffmpeg.Error as exc:
        log.error(f"save_image: ffmpeg error {exc}")
        log_ffmpeg_output(exc.stdout, exc.stderr, as_error=True)
        return False

    return save_path.is_file() and save_path.stat().st_size > 0


def log_ffmpeg_output(stdout, stderr, as_error: bool=False):
    logger = log.error if as_error else log.info
    if stdout or stderr or as_error:
        logger(f"save_image: ffmpeg: stdout: {stdout.decode('utf-8') if stdout else 'No stdout'}")
        logger(f"save_image: ffmpeg: stderr: {stderr.decode('utf-8') if stderr else 'No stderr'}")
    pass


def generate_save_path(camera_id: str, incident_time: datetime, save_folder: Path, file_type_suffix: str) -> Path:
    save_folder.mkdir(parents=True, exist_ok=True)
    save_path = save_folder / f'{camera_id}' / f'{incident_time.strftime("%Y%m%d-%H%M%S")}.{file_type_suffix}'
    save_path.parent.parent.mkdir(exist_ok=True)
    save_path.parent.mkdir(exist_ok=True)
    return save_path


def find_nearest_video(camera_id: str, incident_time: datetime, clip_seconds: int) -> Path:
    # Might actually be in a clip starting around this time, so start at offset-1.
    for offset in range(-1, clip_seconds):
        video_path = generate_save_path(camera_id, incident_time - timedelta(seconds=offset), VIDEO_DIR, 'mp4')
        if video_path.exists():
            return video_path, max(offset, 0)  # zero is the minimum usage offset
    return None, 0


def execute_external_handler(handler_exe: Path, camera_id, relevant_detections: Dict[str, datetime]):
    detection_args = [f'{k}/{dt.strftime("%Y%m%d-%H%M%S")}' for k, dt in relevant_detections.items()]
    if os.path.exists(handler_exe) and os.access(handler_exe, os.F_OK | os.X_OK) and not os.path.isdir(handler_exe):
        log.info(f'Executing handler {handler_exe.as_posix()} {camera_id} {detection_args}')
        Popen([handler_exe.as_posix(), camera_id,] + detection_args)
    else:
        log.critical(f'execute_external_handler: Event executable {handler_exe.as_posix()} is not runnable {camera_id} {detection_args}.')


class EventHandler(ABC):

    def __init__(self, target_camera: TargetCamera):
        super().__init__()
        self.stop_requested = False
        self.target_camera = target_camera
        self.handled: Dict[str, datetime] = {}

    async def _find_rtsp_uri(self, media_service, stream_name) -> str | None:
        rtsp_uri = None
        for profile in await media_service.GetProfiles():
            stream_setup = media_service.create_type('GetStreamUri')
            stream_setup.StreamSetup = {'Stream': 'RTP-Unicast', 'Transport': {'Protocol': 'RTSP'}}
            stream_setup.ProfileToken = profile.token
            uri_data = await media_service.GetStreamUri(stream_setup)
            if profile.Name == stream_name:
                log.info(f'EventHandler: {self.target_camera.config.camera_id} matched {profile.Name=} RTSP {uri_data.Uri=}')
                rtsp_uri = uri_add_authentication(uri_data.Uri,
                                                  self.target_camera.onvif.user,
                                                  self.target_camera.onvif.passwd)
            else:
                log.info(f'EventHandler: {self.target_camera.config.camera_id} skipped {profile.Name=} RTSP {uri_data.Uri=}')
        return rtsp_uri

    def all_been_handled(self, detections: Dict[str, datetime]):  # have all been handled already
        return all(item in self.handled.items() for item in detections.items())

    def any_been_handled(self, detections: Dict[str, datetime]):  # have any been handled already
        return any(item in self.handled.items() for item in detections.items())

    def not_yet_handled(self, detections: Dict[str, datetime]) -> Dict[str, datetime]:
        return {event_type:event_time for event_type, event_time in detections.items()
                if (event_type, event_time) not in self.handled.items()}

    def mark_as_handled(self, detections: Dict[str, datetime]):
        for event, etime in detections.items():
            self.handled[event] = etime

    @abstractmethod
    async def handle_events(self):
        pass

# Loops checking for events and writes out media for any specified as notable
class MediaSaverEventHandler(EventHandler):

    def __init__(self, target_camera: TargetCamera, stream_name):
        super().__init__(target_camera)
        self.camera_id = target_camera.config.camera_id
        self.stream_name = stream_name
        self.log_name = f"{self.__class__.__name__}: {self.camera_id}.{self.stream_name}"
        self.save_path = Path(target_camera.config.camera_save_folder)
        try:
            self.save_path.mkdir(exist_ok=True)
            if not os.access(self.save_path, os.W_OK):
                raise PermissionError(f"path {self.save_path} is not writable")
        except (PermissionError, FileNotFoundError) as file_access_exception:
            log.error(f"{self.log_name}: {str(file_access_exception)}")
            sys.exit(1)
        log.info(f"{self.log_name}: save path: {self.save_path.as_posix()}")

    @abstractmethod
    def get_saver_function(self, rtsp_uri: str, relevant_detections: Dict[str, datetime]) -> Callable:
        assert 'Abstract lacks definition'
        return lambda *args, **kwargs: None

    async def handle_events(self):
        previous_rerr = None
        while not self.stop_requested:
            try:
                media_service = await self.target_camera.onvif.create_media_service()
                log.info(f'{self.log_name}: Trying to connect to stream {self.stream_name}')
                if rtsp_uri := await self._find_rtsp_uri(media_service, self.stream_name):
                    if log.isEnabledFor(logging.DEBUG):   # Note the URI now contains the password!
                        log.debug(f'{self.log_name}: Full URI for {self.stream_name}: {rtsp_uri=}')
                    log.info(f'{self.log_name}: Successfully connected to {self.stream_name}')
                    while not self.stop_requested:  # Loop handling additions to detections made by NotificationPuller
                        # Only save media on relevant non-False events.
                        if relevant_detections := {
                            event_name: etime for event_name, etime in self.target_camera.detections.items()
                            if not event_name.endswith(EVENT_NOT_HAPPENING_SUFFIX)
                               and self.target_camera.config.is_event_targeted(event_name)
                        }:
                            loop = asyncio.get_running_loop()
                            if to_do := self.not_yet_handled(relevant_detections):
                                with ProcessPoolExecutor() as pool:
                                    succeeded = await loop.run_in_executor(
                                        pool,
                                        self.get_saver_function(rtsp_uri, to_do))
                                if succeeded:
                                    self.mark_as_handled(to_do)
                                else:
                                    log.warning(
                                        f"{self.log_name}: mídia/índice não concluído; "
                                        "o evento continuará elegível para nova tentativa."
                                    )
                                    await asyncio.sleep(EXCEPTION_RETRY_WAIT_SECONDS)
                        await asyncio.sleep(0.1)
                    previous_rerr = None
                else:
                    log.info(f"{self.log_name}: Could not connect to stream {self.stream_name}. "
                             f"Is this the correct stream name? "
                             f"Waiting {EXCEPTION_RETRY_WAIT_SECONDS} seconds in case it becomes available.")
                    await asyncio.sleep(EXCEPTION_RETRY_WAIT_SECONDS)
            except ONVIFError as onvif_exception:
                rerr = repr(onvif_exception)
                if rerr != previous_rerr:
                    log.warning(f"{self.log_name}: ONVIF Error (may not be serious): [{repr(onvif_exception)}]")
                    log.info(f'{self.log_name}: Assuming {self.log_name} camera is unavailable, will keep retrying every {EXCEPTION_RETRY_WAIT_SECONDS} seconds.')
                await asyncio.sleep(EXCEPTION_RETRY_WAIT_SECONDS)


class VideoWriter(MediaSaverEventHandler):

    def __init__(self, target_camera: TargetCamera, stream_name: str, clip_seconds: int):
        super().__init__(target_camera, stream_name)
        self.clip_seconds = clip_seconds
        self.people_process = None
        self.people_path = None
        self.people_event_time = None
        self.people_started_at = None
        self.people_absent_since = None

    def get_saver_function(self, rtsp_uri: str, relevant_detections: Dict[str, datetime]) -> Callable:
        return partial(save_video, self.target_camera.config, rtsp_uri, self.clip_seconds, relevant_detections)

    def start_people_recording(self, rtsp_uri: str, event_time: datetime) -> bool:
        started_at = datetime.now()
        path = generate_save_path(
            self.camera_id, started_at, VIDEO_DIR, "mp4"
        )
        if path.exists():
            log.error(f"Gravação recusada: caminho já existe: {path}")
            return False

        try:
            process = ffmpeg.input(
                rtsp_uri, loglevel=24, rtsp_transport="tcp"
            ).output(
                path.as_posix(), f="mpegts",
                vcodec="h264", acodec="aac", preset="ultrafast",
                tune="zerolatency", loglevel=8
            ).run_async(
                pipe_stdout=False, pipe_stderr=False, overwrite_output=True
            )
        except Exception:
            log.exception(f"{self.log_name}: falha ao iniciar gravação contínua")
            path.unlink(missing_ok=True)
            return False

        self.people_process = process
        self.people_path = path
        self.people_event_time = event_time
        self.people_started_at = started_at
        self.people_absent_since = None
        log.info(f"{self.log_name}: gravação contínua iniciada: {path}")
        return True

    def finish_people_recording(self) -> bool:
        process = self.people_process
        path = self.people_path
        event_time = self.people_event_time
        started_at = self.people_started_at
        succeeded = False

        try:
            if process is not None and process.poll() is None:
                process.send_signal(2)
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    log.error(f"{self.log_name}: FFmpeg não encerrou; finalizando processo")
                    process.kill()
                    process.wait()

            valid_file = (
                path is not None and path.is_file() and path.stat().st_size > 0
            )
            if valid_file and event_time is not None and started_at is not None:
                try:
                    publish_video_ready(
                        self.camera_id, "IsPeople", event_time, path, started_at
                    )
                    self.mark_as_handled({"IsPeople": event_time})
                    succeeded = True
                    log.info(f"{self.log_name}: gravação contínua publicada: {path}")
                except Exception:
                    log.exception(f"{self.log_name}: falha ao publicar vídeo de presença")
            else:
                log.error(f"{self.log_name}: gravação contínua inválida ou incompleta")
        finally:
            if not succeeded and path is not None:
                path.unlink(missing_ok=True)

            if self.target_camera.config.is_event_targeted(VIDEO_ENDED_SYNTHETIC_EVENT):
                execute_external_handler(
                    Path(self.target_camera.config.camera_event_exec),
                    self.camera_id,
                    {VIDEO_ENDED_SYNTHETIC_EVENT: datetime.now()}
                )

            self.people_process = None
            self.people_path = None
            self.people_event_time = None
            self.people_started_at = None
            self.people_absent_since = None

        return succeeded

    async def handle_events(self):
        previous_rerr = None
        while not self.stop_requested:
            try:
                media_service = await self.target_camera.onvif.create_media_service()
                log.info(f"{self.log_name}: Trying to connect to stream {self.stream_name}")
                rtsp_uri = await self._find_rtsp_uri(media_service, self.stream_name)

                if not rtsp_uri:
                    log.warning(f"{self.log_name}: stream indisponível; tentando novamente")
                    await asyncio.sleep(EXCEPTION_RETRY_WAIT_SECONDS)
                    continue

                log.info(f"{self.log_name}: Successfully connected to {self.stream_name}")

                while not self.stop_requested:
                    detections = self.target_camera.detections

                    while self.target_camera.presence_events:
                        is_present, event_time = self.target_camera.presence_events.pop(0)

                        if is_present:
                            self.people_absent_since = None
                            if (
                                self.people_process is None
                                and self.handled.get("IsPeople") != event_time
                            ):
                                self.start_people_recording(rtsp_uri, event_time)
                        elif self.people_process is not None:
                            if self.people_absent_since is None:
                                self.people_absent_since = event_time

                    if (
                        self.people_process is not None
                        and self.people_absent_since is not None
                        and (datetime.now() - self.people_absent_since).total_seconds() >= 10
                    ):
                        self.finish_people_recording()

                    if self.people_process is not None and self.people_process.poll() is not None:
                        log.error(f"{self.log_name}: FFmpeg encerrou inesperadamente")
                        if self.people_path is not None:
                            self.people_path.unlink(missing_ok=True)
                        self.people_process = None
                        self.people_path = None
                        self.people_event_time = None
                        self.people_started_at = None
                        self.people_absent_since = None

                    car_detections = {
                        name: event_time
                        for name, event_time in detections.items()
                        if name != "IsPeople"
                        and not name.endswith(EVENT_NOT_HAPPENING_SUFFIX)
                        and self.target_camera.config.is_event_targeted(name)
                    }

                    if car_detections:
                        to_do = self.not_yet_handled(car_detections)
                        if to_do:
                            loop = asyncio.get_running_loop()
                            with ProcessPoolExecutor() as pool:
                                succeeded = await loop.run_in_executor(
                                    pool, self.get_saver_function(rtsp_uri, to_do)
                                )
                            if succeeded:
                                self.mark_as_handled(to_do)
                            else:
                                log.warning(f"{self.log_name}: falha ao salvar clipe; nova tentativa pendente")
                                await asyncio.sleep(EXCEPTION_RETRY_WAIT_SECONDS)

                    await asyncio.sleep(0.1)

                if self.people_process is not None:
                    self.finish_people_recording()
                previous_rerr = None

            except ONVIFError as exc:
                rerr = repr(exc)
                if rerr != previous_rerr:
                    log.warning(f"{self.log_name}: erro ONVIF: {rerr}")
                previous_rerr = rerr
                await asyncio.sleep(EXCEPTION_RETRY_WAIT_SECONDS)

class ImageWriter(MediaSaverEventHandler):

    def get_saver_function(self, rtsp_uri: str, relevant_detections: Dict[str, datetime]) -> Callable:
        # Will capture a new frame for as yet unhandled
        return partial(save_image, self.target_camera.config, rtsp_uri, relevant_detections,
                       self.target_camera.config.camera_grab_stills_from_video)

# Loops checking for events and then executes an external program
class EventExecHandler(EventHandler):

    def __init__(self, target_camera: TargetCamera, handler_exe: Path):
        super().__init__(target_camera)
        self.handler_exe = handler_exe
        self.delay_seconds = self.target_camera.config.camera_event_exec_delay_seconds
        if self.delay_seconds:
            log.info(f'EventExecHandler for {self.target_camera.config.camera_id} subject to delay of {self.delay_seconds} seconds')

    async def handle_events(self):
        current_delay = self.delay_seconds
        while not self.stop_requested:
            if relevant_detections := {event: etime
                                       for event, etime in self.target_camera.detections.items()
                                       if self.target_camera.config.is_event_targeted(event)}:
                if not self.all_been_handled(relevant_detections):
                    if current_delay > 0:  # sleep and see if more events pile in
                        for _ in range(current_delay):
                            if self.stop_requested:
                                break
                            await asyncio.sleep(1.0)
                        current_delay = 0  # don't delay any longer
                    else:
                        loop = asyncio.get_running_loop()
                        with ProcessPoolExecutor() as pool:
                            await loop.run_in_executor(
                                pool,
                                partial(execute_external_handler, self.handler_exe,
                                        self.target_camera.config.camera_id,
                                        relevant_detections))
                        self.mark_as_handled(relevant_detections)
                        current_delay = self.delay_seconds
            await asyncio.sleep(0.1)


async def discover_devices():
    if try_ws_discovery:
        # for some reason this does not work - might be an issue with my network
        wsd = WSDiscovery(ttl=4, relates_to=True)
        wsd.start()
        services = wsd.searchServices(types=[
                    QName(
                        "http://www.onvif.org/ver10/network/wsdl",
                        "NetworkVideoTransmitter",
                        "dp0",
                    )], scopes=[Scope('onvif://www.onvif.org/Profile/Streaming')])
        for service in services:
            log.info(f"discover_devices: {service.getEPR()}: {service.getXAddrs()[0]}")
        wsd.stop()
        log.info('discover_devices: done device device discovery')
        #sys.exit(0)
    else:
        pass

async def main():

    def exit_handler(signum, _):
        log.warning(f'{signal.strsignal(signum)} signalled - exiting')
        notification_puller.stop_requested = True
        video_writer.stop_requested = True
        sys.exit(0)

    signal.signal(signal.SIGHUP, exit_handler)
    signal.signal(signal.SIGINT, exit_handler)

    arg_parser = argparse.ArgumentParser(
        prog='onvifeye',
        description='Monitor a TP-Link Tapo-camera for ONVIF events and record them using RSTP',
        usage='onvifeye.py [--verbose] [config-overrides]',
        epilog='Copyright Michael Hamilton, GPU GNU General Public License v3.0')
    arg_parser.add_argument('-c', '--create-config', type=Path,
                            help='create a new camera config file from arguments')
    arg_parser.add_argument('-v', '--verbose', action='store_true')  # on/off flag

    for key, value in vars(CameraConfig()).items():
        arg_parser.add_argument(f'--{key.replace("_", "-")}', type=type(value), required=False)

    args_namespace = arg_parser.parse_args()
    print(args_namespace)

    if args_namespace.verbose:
        log.setLevel(logging.DEBUG)

    camera_conf_dir = CAMERA_CONFIG_DIR
    camera_conf_dir.mkdir(parents=True, exist_ok=True)

    await discover_devices()  # this doesn't work (at least not on my network).

    camera_configs_list = []

    if args_namespace.create_config:
        save_path = camera_conf_dir / args_namespace.create_config
        if save_path.suffix != '.conf':
            log.error('Save filename does not end in .conf')
            sys.exit(1)
        log.warning(f'Creating config file {save_path.as_posix()} and exiting.'
                    f' Please check it, further customise it for your camera, then restart.')
        camera_config = CameraConfig()
        for arg, value in vars(camera_config).items():  # initialize from command line args - if any
            vars(camera_config)[arg] = value
        with open(save_path, 'w') as fp:
            json.dump(camera_config, fp, default=vars, indent=4)
        sys.exit(0)

    for config_file in [camera_conf_dir / match for match in glob.glob('*.conf', root_dir=camera_conf_dir)]:
        if config_file.is_file():
            log.info(f'Reading config from {config_file.as_posix()}.')
            with open(config_file) as fp:
                camera_config = CameraConfig(**json.load(fp, strict=False))
            for arg, value in vars(args_namespace).items():  # override from command line args - if any
                if value:
                    print(f'{arg=}{value=}')
                    log.warning(f'Overriding {config_file.as_posix()} {arg} with command line value {value}.')
                    vars(camera_config)[arg] = value

            log.debug(f'{vars(camera_config)}')
            camera_configs_list.append(camera_config)

    if not camera_configs_list:
        command_line_config = CameraConfig()
        for arg, value in vars(args_namespace).items():  # override from command line args - if any
            if value:
                vars(camera_config)[arg] = value
        camera_configs_list.append(command_line_config)

    target_camera_list = []
    for camera_config in camera_configs_list:
        target_camera = TargetCamera(camera_config)
        target_camera_list.append(target_camera)

    try:
        async with asyncio.TaskGroup() as watch_task_group:
            for target_camera in target_camera_list:
                notification_puller = NotificationPuller(target_camera)
                _ = watch_task_group.create_task(notification_puller.listen())
                if camera_config.camera_stills_stream_name:
                    if camera_config.camera_grab_stills_from_video:
                        log.warning(f'ImageWriter: {camera_config.camera_id} set to camera_grab_stills_from_video, ignoring stream {camera_config.camera_stills_stream_name}')
                        image_feed = camera_config.camera_stream_name
                    else:
                        image_feed = camera_config.camera_stills_stream_name
                    log.info(f'ImageWriter: {camera_config.camera_id} still-image feed set to {image_feed}')
                    image_writer = ImageWriter(target_camera, stream_name=image_feed)
                    _ = watch_task_group.create_task(image_writer.handle_events())
                if camera_config.camera_stream_name:
                    log.info(f'ImageWriter: {camera_config.camera_id} video feed set to {image_feed}')
                    video_writer = VideoWriter(target_camera,
                                               stream_name=camera_config.camera_stream_name,
                                               clip_seconds=camera_config.camera_clip_seconds)
                    _ = watch_task_group.create_task(video_writer.handle_events())
                if camera_config.camera_event_exec:
                    event_exec_runner = EventExecHandler(target_camera, Path(camera_config.camera_event_exec))
                    _ = watch_task_group.create_task(event_exec_runner.handle_events())
    except* Exception as task_group_exception:
        log.exception(f"Caught taskgroup exception: {task_group_exception.__class__.__name__}: {task_group_exception}")
        # TODO - maybe need to restart?
        # def restart_script():
        #     # sys.executable is the path to the python interpreter
        #     # sys.argv contains the script name and all arguments
        #     os.execv(sys.executable, [sys.executable] + sys.argv)


    loop = asyncio.get_event_loop()
    loop.set_exception_handler(handle_task_exception)

    await watch_task_group


def handle_task_exception(_, context):
    # context["message"] will always be there; but context["exception"] may not
    msg = context.get("message", "No message")
    exc = context.get('exception')
    if exc:
        log.exception(f"Caught task exception: {exc.__class__.__name__}: {exc}")
    else:
        log.error(f"Caught task exception: {msg}")


if __name__ == '__main__':
    if sys.stdin.isatty():  # ffmpeg is doing something to the tty - save attributes for restoration at exit
        tty_fd = sys.stdin.fileno()
        tty_attrs = termios.tcgetattr(tty_fd)
    else:
        tty_fd = None
        tty_attrs = []
    try:
        asyncio.run(main())
    except Exception as asyncio_run_exception:
        trace_text = traceback.format_exc()
        log.error(f'Exiting due to exception {asyncio_run_exception} {trace_text}')
    finally:
        log.info('Cleaning up...')
        if tty_attrs:   # ffmpeg is doing something to the tty - restore it
            termios.tcsetattr(tty_fd, termios.TCSAFLUSH, tty_attrs)
            log.info('Restored tty')
        log.info('Exiting.')
