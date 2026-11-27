import asyncio
import json
import os
import secrets
import sqlite3
import string
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from livekit import api as lk_api
from pydantic import BaseModel

app = FastAPI()

BASE_DIR = Path(__file__).parent
DB_PATH = Path(os.getenv("DB_PATH", str(BASE_DIR / "meetings.db")))
RECORDINGS_DIR = Path(os.getenv("RECORDINGS_DIR", str(BASE_DIR / "recordings")))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
RECORDINGS_DIR.mkdir(parents=True, exist_ok=True)

MAX_USERS_PER_ROOM = 100
MAX_WB_STROKES = 2500

LIVEKIT_URL = os.getenv("LIVEKIT_URL", "ws://localhost:7880")
LIVEKIT_API_KEY = os.getenv("LIVEKIT_API_KEY", "devkey")
LIVEKIT_API_SECRET = os.getenv("LIVEKIT_API_SECRET", "secret")

rooms: dict[str, dict[str, dict]] = {}
waiting_rooms: dict[str, dict[str, dict]] = {}
room_meta: dict[str, dict] = {}

HTML_CONTENT = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover" />
  <meta http-equiv="Cache-Control" content="no-store, no-cache, must-revalidate" />
  <title>Python LiveKit Video Conference (100 Users)</title>
  <script src="https://cdn.jsdelivr.net/npm/livekit-client/dist/livekit-client.umd.min.js"></script>
  <script src="https://cdn.jsdelivr.net/gh/yusitnikov/fix-webm-duration/fix-webm-duration.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/html2canvas@1.4.1/dist/html2canvas.min.js"></script>
  <style>
    * { box-sizing: border-box; font-family: system-ui, -apple-system, sans-serif; }
    body { margin: 0; background: #0f1115; color: #f4f4f5; min-height: 100vh; min-height: 100dvh; display: flex; flex-direction: column; }
    .hidden, .page-hidden { display: none !important; }
    .mobile-only { display: none !important; }
    #lobby-view { max-width: 960px; width: 100%; margin: 2rem auto; padding: 0 1.25rem; display: grid; gap: 1.5rem; }
    #lobby-home-sections { display: grid; gap: 1.5rem; }
    .lobby-header { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 1rem; }
    .lobby-header h1 { margin: 0; font-size: 1.6rem; }
    .profile-setup { display: flex; align-items: center; gap: 0.85rem; background: #181b22; border: 1px solid #2b303b; padding: 0.65rem 0.9rem; border-radius: 12px; flex-wrap: wrap; }
    .avatar-circle { width: 52px; height: 52px; border-radius: 50%; background: #2563eb; color: #fff; font-weight: 700; font-size: 1.15rem; display: flex; align-items: center; justify-content: center; overflow: hidden; flex-shrink: 0; border: 2px solid #3b82f6; transition: box-shadow 0.15s ease, border-color 0.15s ease, transform 0.15s ease; }
    .avatar-circle img { width: 100%; height: 100%; object-fit: cover; }
    .avatar-circle.large { width: 82px; height: 82px; font-size: 1.7rem; }
    .avatar-circle.small { width: 34px; height: 34px; font-size: 0.85rem; border-width: 1.5px; }
    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 1.25rem; }
    .card { background: #181b22; border: 1px solid #2b303b; border-radius: 12px; padding: 1.25rem; display: flex; flex-direction: column; gap: 0.85rem; }
    .card h2 { margin: 0; font-size: 1.1rem; color: #e4e4e7; }
    label { font-size: 0.85rem; color: #a1a1aa; display: block; margin-bottom: 0.3rem; }
    input[type="text"], input[type="datetime-local"], select { width: 100%; padding: 0.6rem 0.75rem; border-radius: 8px; border: 1px solid #333844; background: #101217; color: #fff; font-size: 0.95rem; }
    .checkbox-row { display: flex; align-items: center; gap: 0.55rem; font-size: 0.88rem; color: #d4d4d8; cursor: pointer; margin: 0; }
    .checkbox-row input { width: auto; accent-color: #2563eb; cursor: pointer; }
    button, .btn-link { padding: 0.55rem 0.9rem; border: none; border-radius: 8px; cursor: pointer; font-weight: 600; font-size: 0.88rem; background: #2563eb; color: #fff; text-decoration: none; display: inline-flex; align-items: center; justify-content: center; gap: 4px; touch-action: manipulation; transition: opacity 0.15s, background 0.15s; }
    button:hover:not(:disabled), .btn-link:hover { opacity: 0.9; }
    button:disabled { opacity: 0.45; cursor: not-allowed; }
    button.danger { background: #dc2626; }
    button.secondary, .btn-link.secondary { background: #303642; }
    button.active { background: #16a34a; }
    button.warning { background: #d97706; }
    .row { display: flex; gap: 0.5rem; flex-wrap: wrap; align-items: center; }
    .meeting-list { display: flex; flex-direction: column; gap: 0.75rem; }
    .meeting-item { background: #101217; border: 1px solid #2b303b; border-radius: 8px; padding: 0.9rem 1rem; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 0.75rem; }
    .meeting-meta { display: flex; flex-direction: column; gap: 0.2rem; min-width: 0; flex: 1; }
    .meeting-title { font-weight: 600; font-size: 1rem; word-break: break-word; }
    .meeting-sub { font-size: 0.82rem; color: #a1a1aa; word-break: break-word; }
    .link-box { background: #101217; border: 1px dashed #3b82f6; padding: 0.6rem 0.75rem; border-radius: 8px; font-size: 0.85rem; word-break: break-all; color: #93c5fd; }
    #player-modal { position: fixed; inset: 0; background: rgba(0, 0, 0, 0.85); z-index: 60; display: flex; align-items: center; justify-content: center; padding: 1rem; }
    .player-box { background: #181b22; border: 1px solid #333844; border-radius: 12px; padding: 1rem; width: min(95vw, 960px); display: flex; flex-direction: column; gap: 0.75rem; }
    .player-header { display: flex; justify-content: space-between; align-items: center; gap: 0.75rem; }
    #player-video { width: 100%; max-height: 75dvh; background: #000; border-radius: 8px; }
    #call-view { display: flex; flex-direction: column; height: 100vh; height: 100dvh; overflow: hidden; position: relative; }
    #call-header { order: 1; padding: 0.65rem 1.15rem; background: #181b22; border-bottom: 1px solid #2b303b; display: flex; justify-content: space-between; align-items: center; gap: 0.65rem; flex-shrink: 0; }
    .call-info { display: flex; flex-direction: column; min-width: 0; flex: 1; }
    .call-title { font-weight: 700; font-size: 1rem; display: flex; align-items: center; gap: 0.45rem; flex-wrap: wrap; }
    .host-pill { font-size: 0.7rem; background: #7c3aed; color: #fff; padding: 2px 7px; border-radius: 999px; font-weight: 600; }
    .count-pill { font-size: 0.75rem; background: #272c38; color: #e4e4e7; padding: 2px 8px; border-radius: 999px; }
    .timer-pill { font-size: 0.75rem; background: #1e293b; color: #93c5fd; border: 1px solid #3b82f6; padding: 2px 8px; border-radius: 999px; font-weight: 600; font-variant-numeric: tabular-nums; }
    .timer-pill.warning { background: #451a03; color: #fde68a; border-color: #f59e0b; }
    .timer-pill.danger { background: #450a0a; color: #fecaca; border-color: #ef4444; animation: pulseRec 1.2s infinite; }
    .joining-pill { font-size: 0.73rem; background: #b45309; color: #fff; padding: 2px 8px; border-radius: 999px; font-weight: 600; }
    .rec-pill { font-size: 0.72rem; background: #dc2626; color: #fff; padding: 2px 9px; border-radius: 999px; font-weight: 700; display: inline-flex; align-items: center; gap: 5px; animation: pulseRec 1.4s infinite; }
    @keyframes pulseRec { 0%, 100% { opacity: 1; } 50% { opacity: 0.55; } }
    .call-sub { font-size: 0.78rem; color: #a1a1aa; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .header-actions { display: flex; gap: 0.45rem; align-items: center; flex-shrink: 0; }
    #controls-dock { order: 2; background: #14171d; border-bottom: 1px solid #2b303b; padding: 0.5rem 1.15rem; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 0.5rem; flex-shrink: 0; position: relative; z-index: 25; }
    .primary-controls, .secondary-controls { display: flex; gap: 0.45rem; flex-wrap: wrap; align-items: center; }
    #view-mode-select, #quality-mode-select { width: auto; padding: 0.45rem 0.65rem; font-size: 0.84rem; font-weight: 600; background: #303642; border: 1px solid #454d5e; cursor: pointer; }
    .audio-switch { display: inline-flex; background: #101217; border: 1px solid #333844; border-radius: 8px; padding: 2px; gap: 2px; }
    .audio-switch button { background: transparent; color: #a1a1aa; padding: 0.42rem 0.75rem; border-radius: 6px; font-size: 0.84rem; }
    .audio-switch button.selected-mic { background: #16a34a; color: #fff; }
    .audio-switch button.selected-speaker { background: #2563eb; color: #fff; }
    .badge { background: #ef4444; color: #fff; font-size: 0.72rem; padding: 1px 6px; border-radius: 999px; margin-left: 3px; }
    .badge.amber { background: #d97706; }
    #call-body { order: 3; flex: 1; display: flex; min-height: 0; position: relative; }
    #stage { flex: 1; display: flex; flex-direction: column; position: relative; min-width: 0; overflow: hidden; }
    #mobile-view-bar { display: none; align-items: center; justify-content: center; gap: 0.35rem; padding: 0.35rem 0.5rem; background: #12151b; border-bottom: 1px solid #242934; flex-shrink: 0; overflow-x: auto; }
    .m-view-pill { padding: 0.24rem 0.55rem; font-size: 0.72rem; border-radius: 999px; background: #232834; color: #a1a1aa; white-space: nowrap; }
    .m-view-pill.active-pill { background: #2563eb; color: #fff; }
    .swipe-hint { font-size: 0.68rem; color: #71717a; margin-left: 4px; white-space: nowrap; }
    #view-swipe-toast { position: absolute; top: 52px; left: 50%; transform: translateX(-50%); background: rgba(17, 24, 39, 0.92); border: 1px solid #3b82f6; color: #fff; padding: 0.42rem 0.9rem; border-radius: 999px; font-size: 0.84rem; font-weight: 600; z-index: 40; pointer-events: none; box-shadow: 0 8px 20px rgba(0, 0, 0, 0.45); animation: popIn 0.16s ease-out; }
    @keyframes swipeInNext { from { opacity: 0.55; transform: translateX(30px); } to { opacity: 1; transform: translateX(0); } }
    @keyframes swipeInPrev { from { opacity: 0.55; transform: translateX(-30px); } to { opacity: 1; transform: translateX(0); } }
    #video-grid.swipe-next { animation: swipeInNext 0.22s ease-out; }
    #video-grid.swipe-prev { animation: swipeInPrev 0.22s ease-out; }
    .stage-overlay { position: absolute; inset: 0; background: rgba(10, 12, 16, 0.9); z-index: 30; display: flex; align-items: center; justify-content: center; padding: 1rem; }
    .join-modal { background: #181b22; border: 1px solid #333844; border-radius: 14px; padding: 1.5rem; width: min(94%, 440px); max-height: 88dvh; overflow-y: auto; display: flex; flex-direction: column; align-items: center; gap: 1rem; text-align: center; box-shadow: 0 20px 50px rgba(0, 0, 0, 0.6); }
    .join-modal h2 { margin: 0; font-size: 1.25rem; }
    .join-modal p { margin: 0; font-size: 0.88rem; color: #a1a1aa; }
    .join-choice-grid { display: grid; gap: 0.65rem; width: 100%; }
    .join-choice-btn { padding: 0.85rem 1rem; font-size: 0.95rem; display: flex; align-items: center; justify-content: center; gap: 0.5rem; }
    .spinner { width: 28px; height: 28px; border: 3px solid #333844; border-top-color: #3b82f6; border-radius: 50%; animation: spin 0.9s linear infinite; }
    @keyframes spin { to { transform: rotate(360deg); } }
    #banner-container { position: fixed; top: 64px; left: 50%; transform: translateX(-50%); z-index: 55; display: flex; flex-direction: column; gap: 0.5rem; width: min(92%, 520px); pointer-events: none; }
    .banner-card { pointer-events: auto; background: #1f2430; border: 1px solid #3b82f6; border-radius: 10px; padding: 0.75rem 1rem; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 0.65rem; box-shadow: 0 10px 25px rgba(0, 0, 0, 0.65); animation: popIn 0.2s ease-out; }
    .banner-card.approved { border-color: #16a34a; background: #132a1c; }
    .banner-card.denied { border-color: #dc2626; background: #3b1212; }
    .banner-card.waiting-req { border-color: #f59e0b; background: #261e12; }
    .banner-text { font-size: 0.88rem; line-height: 1.4; display: flex; align-items: center; gap: 0.55rem; }
    #screen-stage { background: #090b0f; border-bottom: 1px solid #2b303b; padding: 0.6rem 1rem; display: flex; flex-direction: column; align-items: center; justify-content: center; max-height: 55dvh; position: relative; flex-shrink: 0; }
    .screen-stage-header { width: 100%; display: flex; justify-content: space-between; align-items: center; gap: 0.75rem; margin-bottom: 0.4rem; flex-shrink: 0; }
    #screen-stage-label { font-size: 0.84rem; color: #93c5fd; font-weight: 600; }
    #screen-fullscreen-btn { padding: 0.3rem 0.7rem; font-size: 0.8rem; flex-shrink: 0; }
    #screen-stage video { max-height: 45dvh; max-width: 100%; border-radius: 8px; background: #000; cursor: pointer; }
    #screen-stage:fullscreen, #screen-stage:-webkit-full-screen { max-height: 100vh; width: 100vw; height: 100vh; padding: 0.45rem 0.75rem; background: #000; justify-content: flex-start; }
    #screen-stage:fullscreen .screen-stage-header, #screen-stage:-webkit-full-screen .screen-stage-header { margin-bottom: 0.35rem; }
    #screen-stage:fullscreen video, #screen-stage:-webkit-full-screen video { flex: 1; min-height: 0; max-height: none; width: 100%; height: 100%; object-fit: contain; border-radius: 0; }
    #whiteboard-stage { background: #14171d; border-bottom: 1px solid #2b303b; padding: 0.55rem 0.85rem; display: flex; flex-direction: column; align-items: center; gap: 0.45rem; max-height: 64dvh; flex-shrink: 0; position: relative; }
    .wb-toolbar { width: 100%; display: flex; align-items: center; gap: 0.4rem; overflow-x: auto; -webkit-overflow-scrolling: touch; padding-bottom: 2px; flex-shrink: 0; }
    .wb-toolbar button { padding: 0.34rem 0.6rem; font-size: 0.78rem; white-space: nowrap; flex-shrink: 0; }
    .wb-toolbar button.wb-tool-active { background: #2563eb; outline: 2px solid #93c5fd; }
    .wb-colors { display: inline-flex; align-items: center; gap: 4px; background: #101217; padding: 3px 6px; border-radius: 8px; border: 1px solid #333844; flex-shrink: 0; }
    .wb-swatch { width: 20px; height: 20px; min-width: 20px; padding: 0; border-radius: 50%; border: 2px solid transparent; }
    .wb-swatch.active-swatch { border-color: #fff; transform: scale(1.15); }
    .wb-canvas-wrap { position: relative; width: 100%; max-width: 960px; aspect-ratio: 16 / 9; max-height: 48dvh; background: #ffffff; border-radius: 8px; overflow: hidden; border: 1px solid #333844; display: flex; align-items: center; justify-content: center; }
    .wb-canvas-wrap canvas { position: absolute; inset: 0; width: 100%; height: 100%; touch-action: none; cursor: crosshair; }
    #wb-preview-canvas { pointer-events: none; }
    .wb-lock-pill { font-size: 0.76rem; color: #fde68a; background: #78350f; padding: 3px 8px; border-radius: 6px; white-space: nowrap; flex-shrink: 0; }
    #whiteboard-stage:fullscreen, #whiteboard-stage:-webkit-full-screen { max-height: 100vh; width: 100vw; height: 100vh; padding: 0.65rem; background: #0f1115; }
    #whiteboard-stage:fullscreen .wb-canvas-wrap, #whiteboard-stage:-webkit-full-screen .wb-canvas-wrap { max-width: none; max-height: calc(100vh - 70px); height: calc(100vh - 70px); width: auto; }
    #video-grid { flex: 1; padding: 0.85rem; overflow-y: auto; -webkit-overflow-scrolling: touch; touch-action: pan-y; }
    #video-grid.mode-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); gap: 0.75rem; align-content: start; justify-items: center; }
    #video-grid.mode-compact { display: grid; grid-template-columns: repeat(auto-fill, minmax(155px, 1fr)); gap: 0.5rem; align-content: start; justify-items: center; }
    #video-grid.mode-compact .avatar-circle.large { width: 46px; height: 46px; font-size: 1rem; }
    #video-grid.mode-compact .ph-text { font-size: 0.72rem; }
    #video-grid.mode-compact .label { font-size: 0.7rem; padding: 2px 6px; bottom: 5px; left: 5px; }
    #video-grid.mode-speaker { display: grid; grid-template-columns: repeat(auto-fill, minmax(175px, 1fr)); grid-auto-rows: max-content; gap: 0.65rem; align-content: start; }
    #video-grid.mode-speaker .video-tile { max-width: none; }
    #video-grid.mode-speaker .video-tile.featured-tile { order: -1; grid-column: 1 / -1; max-height: 55dvh; width: 100%; max-width: 1040px; justify-self: center; }
    #video-grid.mode-sidebar { display: grid; grid-template-columns: minmax(0, 1fr) 230px; grid-auto-rows: min-content; gap: 0.75rem; align-content: start; }
    #video-grid.mode-sidebar.single-tile { grid-template-columns: 1fr; }
    #video-grid.mode-sidebar .video-tile { grid-column: 2; max-width: none; }
    #video-grid.mode-sidebar .video-tile.featured-tile { grid-column: 1; grid-row: 1 / span 24; position: sticky; top: 0; max-width: none; max-height: calc(100dvh - 210px); height: 100%; align-self: stretch; }
    .video-tile { position: relative; width: 100%; max-width: 640px; aspect-ratio: 16 / 9; background: #181b22; border-radius: 10px; overflow: hidden; border: 2px solid #2b303b; display: flex; align-items: center; justify-content: center; contain: layout style; }
    .video-tile.speaking { border-color: #22c55e; box-shadow: 0 0 0 2px rgba(34, 197, 94, 0.55); }
    .video-tile.speaking .placeholder { background: radial-gradient(circle at center, #1b3625 0%, #181b22 72%); }
    .video-tile.speaking .avatar-circle { border-color: #4ade80; transform: scale(1.06); }
    .video-tile.speaking .label { background: rgba(22, 163, 74, 0.92); color: #fff; font-weight: 700; }
    .video-tile video { position: relative; z-index: 0; width: 100%; height: 100%; object-fit: cover; background: #000; }
    .placeholder { position: absolute; inset: 0; z-index: 1; display: flex; flex-direction: column; gap: 0.45rem; align-items: center; justify-content: center; color: #a1a1aa; font-size: 0.85rem; background: #181b22; text-align: center; padding: 0.65rem; }
    .label { position: absolute; bottom: 8px; left: 8px; background: rgba(0, 0, 0, 0.72); padding: 3px 8px; border-radius: 6px; font-size: 0.78rem; z-index: 2; max-width: calc(100% - 16px); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .tile-actions { position: absolute; top: 8px; left: 8px; z-index: 3; display: flex; gap: 4px; align-items: center; }
    .pin-btn, .snap-btn { background: rgba(0, 0, 0, 0.65); color: #e4e4e7; padding: 3px 7px; border-radius: 6px; font-size: 0.75rem; opacity: 0; transition: opacity 0.15s, background 0.15s; }
    .video-tile:hover .pin-btn, .video-tile:hover .snap-btn, .pin-btn.pinned { opacity: 1; }
    .pin-btn.pinned { background: #2563eb; color: #fff; }
    .snap-btn:hover { background: #16a34a; color: #fff; }
    .tile-reaction { position: absolute; top: 8px; right: 8px; background: rgba(0, 0, 0, 0.75); border: 1px solid rgba(255, 255, 255, 0.15); padding: 5px 9px; border-radius: 999px; font-size: 1.3rem; z-index: 3; animation: popIn 0.2s ease-out; }
    #caption-bar { position: absolute; bottom: 58px; left: 50%; transform: translateX(-50%); max-width: min(92%, 760px); background: rgba(0, 0, 0, 0.86); border: 1px solid rgba(255, 255, 255, 0.18); color: #fff; padding: 0.5rem 0.9rem; border-radius: 10px; font-size: 0.95rem; line-height: 1.4; text-align: center; z-index: 15; pointer-events: none; box-shadow: 0 6px 20px rgba(0, 0, 0, 0.5); }
    #caption-speaker { color: #93c5fd; font-weight: 700; margin-right: 6px; }
    #stage-footer { display: flex; justify-content: space-between; align-items: center; gap: 0.5rem; padding: 0.45rem 0.85rem; background: #14171d; border-top: 1px solid #2b303b; flex-shrink: 0; flex-wrap: wrap; }
    #pagination-bar { display: flex; align-items: center; gap: 0.45rem; font-size: 0.82rem; color: #d4d4d8; }
    #pagination-bar button { padding: 0.35rem 0.65rem; font-size: 0.78rem; }
    #reaction-bar { display: flex; justify-content: center; gap: 0.35rem; flex-wrap: wrap; margin: 0 auto; }
    .react-btn { background: #232834; border: 1px solid #323949; border-radius: 999px; padding: 0.35rem 0.68rem; font-size: 1.1rem; line-height: 1; transition: transform 0.12s, background 0.12s; }
    .react-btn:hover { transform: scale(1.15); background: #303746; }
    #reaction-layer { position: absolute; inset: 0; pointer-events: none; overflow: hidden; z-index: 10; }
    .floating-reaction { position: absolute; bottom: 60px; display: flex; flex-direction: column; align-items: center; gap: 2px; animation: floatUp 2.6s ease-out forwards; }
    .floating-reaction .emoji { font-size: 2.2rem; }
    .floating-reaction .who { font-size: 0.72rem; background: rgba(0, 0, 0, 0.75); padding: 2px 7px; border-radius: 999px; color: #e4e4e7; }
    @keyframes floatUp { 0% { transform: translateY(0) scale(0.7); opacity: 0; } 15% { transform: translateY(-20px) scale(1.1); opacity: 1; } 100% { transform: translateY(-240px) scale(1); opacity: 0; } }
    @keyframes popIn { 0% { transform: scale(0.85); opacity: 0; } 100% { transform: scale(1); opacity: 1; } }
    .side-panel { width: 340px; background: #181b22; border-left: 1px solid #2b303b; display: flex; flex-direction: column; flex-shrink: 0; height: 100%; }
    .panel-header { padding: 0.75rem 0.9rem; border-bottom: 1px solid #2b303b; display: flex; justify-content: space-between; align-items: center; font-weight: 700; font-size: 0.95rem; }
    .panel-scroll { flex: 1; overflow-y: auto; padding: 0.75rem; display: flex; flex-direction: column; gap: 0.6rem; }
    .p-section-title { font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.04em; color: #a1a1aa; display: flex; justify-content: space-between; align-items: center; margin-top: 0.25rem; }
    .participant-row { background: #101217; border: 1px solid #2b303b; border-radius: 8px; padding: 0.5rem 0.65rem; display: flex; align-items: center; justify-content: space-between; gap: 0.55rem; }
    .participant-row.speaking { border-color: #22c55e; background: #13281b; }
    .participant-row.speaking .avatar-circle { border-color: #4ade80; }
    .participant-left { display: flex; align-items: center; gap: 0.55rem; min-width: 0; flex: 1; }
    .participant-name-wrap { display: flex; flex-direction: column; min-width: 0; }
    .participant-name { font-size: 0.88rem; font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .participant-tags { display: flex; gap: 4px; flex-wrap: wrap; margin-top: 2px; }
    .mini-tag { font-size: 0.65rem; padding: 1px 5px; border-radius: 4px; background: #272c38; color: #d4d4d8; }
    .mini-tag.host { background: #7c3aed; color: #fff; }
    .mini-tag.screen { background: #16a34a; color: #fff; }
    .participant-right { display: flex; align-items: center; gap: 0.35rem; flex-shrink: 0; font-size: 0.88rem; }
    .waiting-banner-box { background: #261e12; border: 1px solid #d97706; border-radius: 8px; padding: 0.65rem; font-size: 0.84rem; color: #fde68a; }
    .chat-tabs-bar { display: flex; align-items: center; border-bottom: 1px solid #2b303b; }
    .chat-tabs { display: flex; flex: 1; }
    .chat-tab { flex: 1; background: transparent; border-radius: 0; padding: 0.75rem 0.5rem; color: #a1a1aa; border-bottom: 2px solid transparent; font-size: 0.88rem; }
    .chat-tab.active-tab { color: #fff; border-bottom-color: #2563eb; background: #1e222b; }
    .dm-bar { padding: 0.65rem 0.75rem; border-bottom: 1px solid #2b303b; background: #14171d; }
    .chat-messages { flex: 1; overflow-y: auto; padding: 0.85rem; display: flex; flex-direction: column; gap: 0.65rem; }
    .msg-item { display: flex; flex-direction: column; max-width: 88%; align-self: flex-start; }
    .msg-item.self { align-self: flex-end; }
    .msg-meta { font-size: 0.72rem; color: #a1a1aa; margin-bottom: 3px; }
    .msg-item.self .msg-meta { text-align: right; }
    .msg-bubble { background: #272c38; color: #f4f4f5; padding: 0.5rem 0.75rem; border-radius: 10px; font-size: 0.88rem; line-height: 1.35; word-break: break-word; }
    .msg-item.self .msg-bubble { background: #2563eb; color: #fff; }
    .msg-item.dm .msg-bubble { border: 1px solid #9333ea; }
    .msg-item.dm:not(.self) .msg-bubble { background: #2e1f47; cursor: pointer; }
    .dm-tag { display: inline-block; font-size: 0.68rem; background: #7e22ce; color: #fff; padding: 1px 5px; border-radius: 4px; margin-right: 4px; }
    .chat-input-row { padding: 0.7rem; border-top: 1px solid #2b303b; display: flex; gap: 0.5rem; background: #14171d; }
    .chat-input-row input { flex: 1; padding: 0.5rem 0.7rem; font-size: 0.9rem; }
    @media (max-width: 768px) {
      .hide-sm { display: none !important; }
      .mobile-only { display: inline-flex !important; }
      #mobile-view-bar { display: flex; }
      input[type="text"], input[type="datetime-local"], select { font-size: 16px; }
      #lobby-view { margin: 1rem auto; padding: 0 0.85rem; gap: 1rem; }
      .lobby-header h1 { font-size: 1.25rem; }
      .profile-setup { width: 100%; }
      #call-header { padding: 0.5rem 0.75rem; gap: 0.45rem; }
      .call-title { font-size: 0.92rem; gap: 0.35rem; }
      .call-sub { display: none; }
      .header-actions button { padding: 0.4rem 0.65rem; font-size: 0.8rem; }
      #controls-dock { order: 4; z-index: 49; border-bottom: none; border-top: 1px solid #2b303b; padding: 0.45rem 0.55rem calc(0.45rem + env(safe-area-inset-bottom)); justify-content: center; }
      .primary-controls { width: 100%; justify-content: space-between; gap: 0.3rem; flex-wrap: nowrap; }
      .primary-controls .audio-switch { flex: 1.3; justify-content: center; }
      .primary-controls .audio-switch button { flex: 1; padding: 0.5rem 0.3rem; font-size: 0.76rem; }
      .primary-controls > button { flex: 1; padding: 0.52rem 0.35rem; font-size: 0.76rem; }
      #mute-speaker-btn, #mobile-more-btn { flex: 0 0 auto; white-space: nowrap; padding: 0.52rem 0.6rem; }
      #controls-dock.more-open #mobile-more-btn { background: #16a34a; }
      .secondary-controls { display: none; position: fixed; bottom: 60px; left: 0; right: 0; z-index: 48; max-height: 65dvh; overflow-y: auto; background: #1c2029; border-top: 1px solid #333844; padding: 0.75rem; gap: 0.5rem; flex-wrap: wrap; justify-content: center; box-shadow: 0 -10px 30px rgba(0, 0, 0, 0.65); }
      #controls-dock.more-open .secondary-controls { display: flex; }
      .secondary-controls button, .secondary-controls select { flex: 1 1 44%; font-size: 0.84rem; padding: 0.55rem 0.65rem; }
      #video-grid { padding: 0.5rem; }
      #video-grid.mode-grid { grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 0.5rem; }
      #video-grid.mode-compact { grid-template-columns: repeat(auto-fill, minmax(105px, 1fr)); gap: 0.4rem; }
      #video-grid.mode-speaker { grid-template-columns: repeat(auto-fill, minmax(130px, 1fr)); gap: 0.45rem; }
      #video-grid.mode-speaker .video-tile.featured-tile { max-height: 36dvh; }
      #video-grid.mode-sidebar { grid-template-columns: minmax(0, 1fr) 110px; gap: 0.45rem; }
      #video-grid.mode-sidebar .video-tile { grid-column: 2; }
      #video-grid.mode-sidebar .video-tile.featured-tile { grid-column: 1; grid-row: 1 / span 24; position: sticky; top: 0; max-height: 44dvh; }
      .pin-btn, .snap-btn { opacity: 0.85; padding: 2px 6px; font-size: 0.7rem; }
      .avatar-circle.large { width: 54px; height: 54px; font-size: 1.15rem; }
      .placeholder { font-size: 0.75rem; gap: 0.3rem; }
      .label { font-size: 0.72rem; padding: 2px 6px; bottom: 5px; left: 5px; }
      #screen-stage { max-height: 40dvh; padding: 0.45rem 0.6rem; }
      #screen-stage video { max-height: 31dvh; }
      #whiteboard-stage { max-height: 54dvh; padding: 0.45rem 0.55rem; }
      .wb-canvas-wrap { max-height: 36dvh; }
      #stage-footer { padding: 0.35rem 0.5rem; gap: 0.35rem; }
      .react-btn { padding: 0.3rem 0.55rem; font-size: 1rem; }
      #caption-bar { bottom: 50px; font-size: 0.84rem; padding: 0.4rem 0.75rem; }
      .side-panel { position: absolute; inset: 0; width: 100%; max-width: 100%; z-index: 35; }
    }
  </style>
</head>
<body>
  <div id="lobby-view">
    <div class="lobby-header">
      <h1>Video Conferencing (Up to 100 Users)</h1>
      <div class="profile-setup">
        <div id="profile-avatar-preview" class="avatar-circle">U</div>
        <div style="flex: 1; min-width: 150px;"><label for="name-input" style="margin-bottom: 2px;">Your Display Name</label><input id="name-input" type="text" placeholder="Enter your name" style="padding: 0.45rem 0.65rem;" /></div>
        <div class="row"><input id="avatar-file-input" type="file" accept="image/*" class="hidden" /><button id="upload-avatar-btn" class="secondary" type="button" style="padding: 0.45rem 0.75rem;">🖼️ Photo</button><button id="clear-avatar-btn" class="danger hidden" type="button" style="padding: 0.45rem 0.65rem;" title="Remove Photo">✕</button></div>
      </div>
    </div>
    <div id="direct-room-card" class="card hidden">
      <h2 id="direct-room-title">Ready to join meeting</h2>
      <div id="direct-room-time" class="meeting-sub"></div>
      <div id="direct-room-link" class="link-box"></div>
      <div class="row"><button id="direct-join-btn">Join Meeting Now</button><button id="direct-copy-btn" class="secondary">Copy Link</button></div>
    </div>
    <div id="lobby-home-sections">
      <div class="cards">
        <div class="card">
          <h2>Start or Join a Meeting</h2>
          <div><label for="instant-join-mode">Room Joining Permission</label><select id="instant-join-mode"><option value="direct">Direct Join (No Owner Approval)</option><option value="approval">Need Owner's Approval (Waiting Room)</option></select></div>
          <div><label for="instant-duration">Meeting Duration</label><select id="instant-duration"><option value="0">Unlimited (No Time Limit)</option><option value="15">15 Minutes</option><option value="30">30 Minutes</option><option value="45">45 Minutes</option><option value="60">1 Hour (60 Minutes)</option><option value="90">1.5 Hours (90 Minutes)</option><option value="120">2 Hours</option><option value="180">3 Hours</option><option value="240">4 Hours</option><option value="300">5 Hours</option><option value="360">6 Hours</option><option value="420">7 Hours</option><option value="480">8 Hours</option><option value="540">9 Hours</option><option value="600">10 Hours</option><option value="660">11 Hours</option><option value="720">12 Hours</option></select></div>
          <label class="checkbox-row"><input id="instant-approval-chk" type="checkbox" checked /> Require host approval to share screen</label>
          <label class="checkbox-row"><input id="instant-record-chk" type="checkbox" /> Record this meeting automatically</label>
          <button id="instant-btn" class="active">New Instant Meeting</button>
          <hr style="border: 0; border-top: 1px solid #2b303b; width: 100%; margin: 0.25rem 0;" />
          <div><label for="room-input">Join with Meeting Code or Link</label><div class="row"><input id="room-input" type="text" placeholder="e.g. abc-defg-hij" style="flex: 1;" /><button id="join-btn">Join</button></div></div>
        </div>
        <div class="card">
          <h2>Schedule a Meeting</h2>
          <div><label for="sched-title">Meeting Title</label><input id="sched-title" type="text" placeholder="e.g. All-Hands (100 Users)" /></div>
          <div><label for="sched-time">Date & Time</label><input id="sched-time" type="datetime-local" /></div>
          <div><label for="sched-duration">Meeting Duration</label><select id="sched-duration"><option value="0">Unlimited (No Time Limit)</option><option value="15">15 Minutes</option><option value="30">30 Minutes</option><option value="45">45 Minutes</option><option value="60">1 Hour (60 Minutes)</option><option value="90">1.5 Hours (90 Minutes)</option><option value="120">2 Hours</option><option value="180">3 Hours</option><option value="240">4 Hours</option><option value="300">5 Hours</option><option value="360">6 Hours</option><option value="420">7 Hours</option><option value="480">8 Hours</option><option value="540">9 Hours</option><option value="600">10 Hours</option><option value="660">11 Hours</option><option value="720">12 Hours</option></select></div>
          <div><label for="sched-join-mode">Room Joining Permission</label><select id="sched-join-mode"><option value="direct">Direct Join (No Owner Approval)</option><option value="approval">Need Owner's Approval (Waiting Room)</option></select></div>
          <label class="checkbox-row"><input id="sched-approval-chk" type="checkbox" checked /> Require host approval to share screen</label>
          <label class="checkbox-row"><input id="sched-record-chk" type="checkbox" /> Record this meeting automatically</label>
          <button id="sched-btn">Create Scheduled Link</button>
          <div id="sched-result" class="hidden"><div class="link-box" id="sched-link-box"></div></div>
        </div>
      </div>
      <div id="scheduled-meetings-card" class="card hidden"><div class="row" style="justify-content: space-between;"><h2>Scheduled Meetings</h2><button id="refresh-list-btn" class="secondary" style="padding: 0.35rem 0.75rem;">Refresh</button></div><div id="meeting-list" class="meeting-list"><div class="meeting-sub">Loading meetings...</div></div></div>
      <div id="saved-recordings-card" class="card hidden"><div class="row" style="justify-content: space-between;"><h2>Saved Recordings (recordings/)</h2><button id="refresh-rec-btn" class="secondary" style="padding: 0.35rem 0.75rem;">Refresh</button></div><div id="recording-list" class="meeting-list"><div class="meeting-sub">Loading recordings...</div></div></div>
    </div>
  </div>

  <div id="player-modal" class="hidden"><div class="player-box"><div class="player-header"><strong id="player-title">Recording</strong><button id="player-close-btn" class="danger" style="padding: 0.35rem 0.75rem;">✕ Close</button></div><video id="player-video" controls autoplay playsinline></video></div></div>

  <div id="call-view" class="hidden">
    <div id="banner-container"></div>
    <header id="call-header">
      <div class="call-info">
        <span class="call-title"><span id="call-title-text">Meeting</span><span id="host-badge" class="host-pill hidden">Host</span><span id="participant-count" class="count-pill">👥 1 / 100</span><span id="timer-pill" class="timer-pill hidden">⏱️ 00:00</span><button id="extend-time-btn" class="secondary hidden" type="button" style="padding: 2px 7px; font-size: 0.72rem;" title="Extend meeting by 15 minutes">+15m</button><span id="joining-count-pill" class="joining-pill hidden">⏳ 0</span><span id="recording-pill" class="rec-pill hidden">● REC</span></span>
        <span class="call-sub" id="call-sub"></span>
      </div>
      <div class="header-actions"><button id="copy-call-link-btn" class="secondary">🔗 <span class="hide-sm">Copy Link</span></button><button id="leave-btn" class="danger">Leave</button></div>
    </header>
    <div id="controls-dock">
      <div class="primary-controls">
        <div class="audio-switch" title="Switch audio mode"><button id="use-mic-btn" type="button">🎤 <span class="hide-sm">With </span>Mic</button><button id="use-speaker-btn" type="button">🔊 Speaker<span class="hide-sm"> Only</span></button></div>
        <button id="mute-speaker-btn" class="secondary" type="button" title="Mute or unmute incoming speaker audio">🔊 <span class="hide-sm">Mute Speaker</span></button>
        <button id="cam-btn" class="secondary">📷 <span class="hide-sm">Enable </span>Cam</button>
        <button id="toggle-participants-btn" class="secondary">👥 <span class="hide-sm">Participants </span>(<span id="participants-btn-count">1</span>)<span id="joining-btn-badge" class="badge amber hidden">0</span></button>
        <button id="toggle-chat-btn" class="secondary">💬 <span class="hide-sm">Chat</span><span id="chat-total-badge" class="badge hidden">0</span></button>
        <button id="mobile-more-btn" class="secondary mobile-only" type="button">⋯ More</button>
      </div>
      <div class="secondary-controls">
        <select id="view-mode-select" title="Switch Meeting View"><option value="grid">📐 Grid View</option><option value="speaker">🎤 Speaker View</option><option value="sidebar">◫ Sidebar View</option><option value="compact">🪪 Compact View</option></select>
        <select id="quality-mode-select" title="Video Quality & Smoothness"><option value="auto">🎥 Auto HD (Smooth)</option><option value="saver">📶 Data Saver (360p)</option></select>
        <button id="snapshot-btn" class="secondary" title="Take clean snapshot of video stage">📸 Snapshot</button>
        <button id="wb-toggle-btn" class="secondary">🎨 Whiteboard</button>
        <button id="screen-btn" class="secondary">Share Screen</button>
        <button id="cc-btn" class="secondary">💬 CC: Off</button>
        <button id="host-record-btn" class="secondary hidden">🔴 Record</button>
        <button id="host-join-policy-btn" class="secondary hidden"></button>
        <button id="host-policy-btn" class="secondary hidden"></button>
        <button id="host-stop-share-btn" class="danger hidden">Stop Share</button>
      </div>
    </div>
    <div id="call-body">
      <div id="stage">
        <div id="view-swipe-toast" class="hidden"></div>
        <div id="join-options-overlay" class="stage-overlay hidden"><div class="join-modal"><div id="modal-avatar-preview" class="avatar-circle large">U</div><h2 id="join-modal-title">Choose Audio & Camera</h2><p>Select how you want to join this meeting. You can switch anytime on screen.</p><div class="join-choice-grid"><button id="modal-mic-btn" class="active join-choice-btn">🎤 Join with Microphone</button><button id="modal-speaker-btn" class="join-choice-btn">🔊 Join with Only Computer Speaker</button></div><hr style="border: 0; border-top: 1px solid #2b303b; width: 100%; margin: 0.2rem 0;" /><div class="row" style="justify-content: center; width: 100%;"><button id="modal-cam-btn" class="secondary" style="width: 100%;">📷 Enable Camera</button></div></div></div>
        <div id="waiting-room-overlay" class="stage-overlay hidden"><div class="join-modal"><div id="waiting-avatar-preview" class="avatar-circle large">U</div><div class="spinner"></div><h2>Waiting Room</h2><p id="waiting-room-status">Waiting for the meeting owner to let you in...</p><button id="cancel-waiting-btn" class="danger" style="width: 100%;">Leave Waiting Room</button></div></div>
        <div id="reaction-layer"></div>
        <div id="audio-container" class="hidden"></div>
        <div id="mobile-view-bar"><button type="button" class="m-view-pill" data-mode="grid">📐 Grid</button><button type="button" class="m-view-pill" data-mode="speaker">🎤 Speaker</button><button type="button" class="m-view-pill" data-mode="sidebar">◫ Sidebar</button><button type="button" class="m-view-pill" data-mode="compact">🪪 Compact</button><span class="swipe-hint">⇄ Swipe</span></div>
        <div id="screen-stage" class="hidden"><div class="screen-stage-header"><div id="screen-stage-label">Screen Share</div><button id="screen-fullscreen-btn" class="secondary hidden" type="button">⛶ Fullscreen</button></div><video id="screen-stage-video" autoplay playsinline title="Double-click for fullscreen"></video></div>
        <div id="whiteboard-stage" class="hidden">
          <div class="wb-toolbar">
            <button class="secondary wb-tool wb-tool-active" data-tool="pen">✏️ Pen</button><button class="secondary wb-tool" data-tool="highlighter">🖍️ Marker</button><button class="secondary wb-tool" data-tool="line">📏 Line</button><button class="secondary wb-tool" data-tool="rect">⬜ Rect</button><button class="secondary wb-tool" data-tool="circle">⭕ Circle</button><button class="secondary wb-tool" data-tool="text">🔤 Text</button><button class="secondary wb-tool" data-tool="eraser">🧹 Eraser</button>
            <div class="wb-colors"><button class="wb-swatch active-swatch" data-color="#111827" style="background:#111827;"></button><button class="wb-swatch" data-color="#dc2626" style="background:#dc2626;"></button><button class="wb-swatch" data-color="#2563eb" style="background:#2563eb;"></button><button class="wb-swatch" data-color="#16a34a" style="background:#16a34a;"></button><button class="wb-swatch" data-color="#d97706" style="background:#d97706;"></button><button class="wb-swatch" data-color="#9333ea" style="background:#9333ea;"></button></div>
            <select id="wb-size" style="width:auto; padding:0.3rem 0.5rem; font-size:0.78rem;"><option value="3">Thin</option><option value="6" selected>Medium</option><option value="12">Thick</option></select>
            <button id="wb-undo-btn" class="secondary" type="button">↩️ Undo</button><button id="wb-clear-btn" class="danger" type="button">🗑️ Clear</button><button id="wb-save-btn" class="secondary" type="button">💾 Save</button><button id="wb-lock-btn" class="secondary hidden" type="button">🔓 Everyone Draws</button><span id="wb-locked-pill" class="wb-lock-pill hidden">🔒 Host Only</span><button id="wb-fullscreen-btn" class="secondary" type="button">⛶ Fullscreen</button><button id="wb-close-btn" class="danger" type="button">✕ Close</button>
          </div>
          <div class="wb-canvas-wrap"><canvas id="wb-canvas" width="1600" height="900"></canvas><canvas id="wb-preview-canvas" width="1600" height="900"></canvas></div>
        </div>
        <main id="video-grid" class="mode-grid"></main>
        <div id="caption-bar" class="hidden"><span id="caption-speaker"></span><span id="caption-text">Auto Captions Enabled (listening...)</span></div>
        <div id="stage-footer">
          <div id="pagination-bar" class="hidden"><button id="prev-page-btn" class="secondary">◀ Prev</button><span id="page-indicator">Page 1 / 1</span><button id="next-page-btn" class="secondary">Next ▶</button></div>
          <div id="reaction-bar"><button class="react-btn" data-emoji="👍">👍</button><button class="react-btn" data-emoji="❤️">❤️</button><button class="react-btn" data-emoji="😂">😂</button><button class="react-btn" data-emoji="👏">👏</button><button class="react-btn" data-emoji="🎉">🎉</button><button class="react-btn" data-emoji="😮">😮</button></div>
        </div>
      </div>
      <aside id="participants-panel" class="side-panel hidden"><div class="panel-header"><span id="participants-panel-title">Participants (1)</span><button id="close-participants-btn" class="secondary" style="padding: 0.25rem 0.55rem;">✕</button></div><div style="padding: 0.65rem 0.75rem; border-bottom: 1px solid #2b303b;"><input id="participant-search" type="text" placeholder="Search participants..." style="padding: 0.45rem 0.65rem;" /></div><div id="participants-list" class="panel-scroll"></div></aside>
      <aside id="chat-panel" class="side-panel hidden"><div class="chat-tabs-bar"><div class="chat-tabs"><button id="tab-public" class="chat-tab active-tab">Public <span id="badge-public" class="badge hidden">0</span></button><button id="tab-private" class="chat-tab">Personal (DM) <span id="badge-private" class="badge hidden">0</span></button></div><button id="close-chat-btn" class="secondary" style="margin: 0 0.5rem; padding: 0.25rem 0.55rem;">✕</button></div><div id="dm-bar" class="dm-bar hidden"><label for="dm-target" style="margin-bottom: 4px;">Send Private Message To:</label><select id="dm-target"></select></div><div id="public-messages" class="chat-messages"></div><div id="private-messages" class="chat-messages hidden"></div><div class="chat-input-row"><input id="chat-input" type="text" placeholder="Message everyone..." /><button id="send-chat-btn">Send</button></div></aside>
    </div>
  </div>

  <script>
    const VIEW_MODES = ["grid", "speaker", "sidebar", "compact"];
    const VIEW_LABELS = { grid: "📐 Grid View", speaker: "🎤 Speaker View", sidebar: "◫ Sidebar View", compact: "🪪 Compact View" };
    const MIC_CONSTRAINTS = { echoCancellation: true, noiseSuppression: true, autoGainControl: true, channelCount: 1, sampleRate: 48000 };

    const lobbyView = document.getElementById("lobby-view");
    const lobbyHomeSections = document.getElementById("lobby-home-sections");
    const scheduledMeetingsCard = document.getElementById("scheduled-meetings-card");
    const savedRecordingsCard = document.getElementById("saved-recordings-card");
    const callView = document.getElementById("call-view");
    const nameInput = document.getElementById("name-input");
    const profileAvatarPreview = document.getElementById("profile-avatar-preview");
    const avatarFileInput = document.getElementById("avatar-file-input");
    const uploadAvatarBtn = document.getElementById("upload-avatar-btn");
    const clearAvatarBtn = document.getElementById("clear-avatar-btn");
    const roomInput = document.getElementById("room-input");
    const joinBtn = document.getElementById("join-btn");
    const instantBtn = document.getElementById("instant-btn");
    const instantJoinMode = document.getElementById("instant-join-mode");
    const instantDuration = document.getElementById("instant-duration");
    const instantApprovalChk = document.getElementById("instant-approval-chk");
    const instantRecordChk = document.getElementById("instant-record-chk");
    const directRoomCard = document.getElementById("direct-room-card");
    const directRoomTitle = document.getElementById("direct-room-title");
    const directRoomTime = document.getElementById("direct-room-time");
    const directRoomLink = document.getElementById("direct-room-link");
    const directJoinBtn = document.getElementById("direct-join-btn");
    const directCopyBtn = document.getElementById("direct-copy-btn");
    const schedTitle = document.getElementById("sched-title");
    const schedTime = document.getElementById("sched-time");
    const schedDuration = document.getElementById("sched-duration");
    const schedJoinMode = document.getElementById("sched-join-mode");
    const schedApprovalChk = document.getElementById("sched-approval-chk");
    const schedRecordChk = document.getElementById("sched-record-chk");
    const schedBtn = document.getElementById("sched-btn");
    const schedResult = document.getElementById("sched-result");
    const schedLinkBox = document.getElementById("sched-link-box");
    const meetingList = document.getElementById("meeting-list");
    const refreshListBtn = document.getElementById("refresh-list-btn");
    const recordingList = document.getElementById("recording-list");
    const refreshRecBtn = document.getElementById("refresh-rec-btn");
    const playerModal = document.getElementById("player-modal");
    const playerTitle = document.getElementById("player-title");
    const playerVideo = document.getElementById("player-video");
    const playerCloseBtn = document.getElementById("player-close-btn");

    const callTitleText = document.getElementById("call-title-text");
    const hostBadge = document.getElementById("host-badge");
    const participantCountEl = document.getElementById("participant-count");
    const timerPill = document.getElementById("timer-pill");
    const extendTimeBtn = document.getElementById("extend-time-btn");
    const joiningCountPill = document.getElementById("joining-count-pill");
    const recordingPill = document.getElementById("recording-pill");
    const callSub = document.getElementById("call-sub");
    const copyCallLinkBtn = document.getElementById("copy-call-link-btn");
    const controlsDock = document.getElementById("controls-dock");
    const mobileMoreBtn = document.getElementById("mobile-more-btn");
    const hostRecordBtn = document.getElementById("host-record-btn");
    const hostJoinPolicyBtn = document.getElementById("host-join-policy-btn");
    const hostPolicyBtn = document.getElementById("host-policy-btn");
    const hostStopShareBtn = document.getElementById("host-stop-share-btn");
    const viewModeSelect = document.getElementById("view-mode-select");
    const qualityModeSelect = document.getElementById("quality-mode-select");
    const viewSwipeToast = document.getElementById("view-swipe-toast");
    const useMicBtn = document.getElementById("use-mic-btn");
    const useSpeakerBtn = document.getElementById("use-speaker-btn");
    const muteSpeakerBtn = document.getElementById("mute-speaker-btn");
    const camBtn = document.getElementById("cam-btn");
    const snapshotBtn = document.getElementById("snapshot-btn");
    const wbToggleBtn = document.getElementById("wb-toggle-btn");
    const screenBtn = document.getElementById("screen-btn");
    const ccBtn = document.getElementById("cc-btn");
    const toggleParticipantsBtn = document.getElementById("toggle-participants-btn");
    const participantsBtnCount = document.getElementById("participants-btn-count");
    const joiningBtnBadge = document.getElementById("joining-btn-badge");
    const toggleChatBtn = document.getElementById("toggle-chat-btn");
    const chatTotalBadge = document.getElementById("chat-total-badge");
    const leaveBtn = document.getElementById("leave-btn");
    const audioContainer = document.getElementById("audio-container");
    const screenStage = document.getElementById("screen-stage");
    const screenStageLabel = document.getElementById("screen-stage-label");
    const screenStageVideo = document.getElementById("screen-stage-video");
    const screenFullscreenBtn = document.getElementById("screen-fullscreen-btn");
    const whiteboardStage = document.getElementById("whiteboard-stage");
    const wbCanvas = document.getElementById("wb-canvas");
    const wbPreviewCanvas = document.getElementById("wb-preview-canvas");
    const wbCtx = wbCanvas.getContext("2d");
    const wbPreviewCtx = wbPreviewCanvas.getContext("2d");
    const wbSizeSelect = document.getElementById("wb-size");
    const wbUndoBtn = document.getElementById("wb-undo-btn");
    const wbClearBtn = document.getElementById("wb-clear-btn");
    const wbSaveBtn = document.getElementById("wb-save-btn");
    const wbLockBtn = document.getElementById("wb-lock-btn");
    const wbLockedPill = document.getElementById("wb-locked-pill");
    const wbFullscreenBtn = document.getElementById("wb-fullscreen-btn");
    const wbCloseBtn = document.getElementById("wb-close-btn");
    const videoGrid = document.getElementById("video-grid");
    const captionBar = document.getElementById("caption-bar");
    const captionSpeaker = document.getElementById("caption-speaker");
    const captionText = document.getElementById("caption-text");
    const paginationBar = document.getElementById("pagination-bar");
    const prevPageBtn = document.getElementById("prev-page-btn");
    const nextPageBtn = document.getElementById("next-page-btn");
    const pageIndicator = document.getElementById("page-indicator");
    const reactionLayer = document.getElementById("reaction-layer");
    const bannerContainer = document.getElementById("banner-container");
    const joinOptionsOverlay = document.getElementById("join-options-overlay");
    const modalAvatarPreview = document.getElementById("modal-avatar-preview");
    const joinModalTitle = document.getElementById("join-modal-title");
    const modalMicBtn = document.getElementById("modal-mic-btn");
    const modalSpeakerBtn = document.getElementById("modal-speaker-btn");
    const modalCamBtn = document.getElementById("modal-cam-btn");
    const waitingRoomOverlay = document.getElementById("waiting-room-overlay");
    const waitingAvatarPreview = document.getElementById("waiting-avatar-preview");
    const waitingRoomStatus = document.getElementById("waiting-room-status");
    const cancelWaitingBtn = document.getElementById("cancel-waiting-btn");
    const participantsPanel = document.getElementById("participants-panel");
    const participantsPanelTitle = document.getElementById("participants-panel-title");
    const closeParticipantsBtn = document.getElementById("close-participants-btn");
    const participantSearch = document.getElementById("participant-search");
    const participantsList = document.getElementById("participants-list");
    const chatPanel = document.getElementById("chat-panel");
    const closeChatBtn = document.getElementById("close-chat-btn");
    const tabPublic = document.getElementById("tab-public");
    const tabPrivate = document.getElementById("tab-private");
    const badgePublic = document.getElementById("badge-public");
    const badgePrivate = document.getElementById("badge-private");
    const dmBar = document.getElementById("dm-bar");
    const dmTarget = document.getElementById("dm-target");
    const publicMessages = document.getElementById("public-messages");
    const privateMessages = document.getElementById("private-messages");
    const chatInput = document.getElementById("chat-input");
    const sendChatBtn = document.getElementById("send-chat-btn");

    const clientId = Math.random().toString(36).substring(2, 10) + Math.random().toString(36).substring(2, 6);
    let rejoinToken = "";

    let isMainHostServer = false, currentRoomId = null, currentMeetingTitle = "Meeting", maxUsersInRoom = 100;
    let ws = null, wsPingTimer = null, wsReconnectTimer = null, isLeavingCall = false, lkRoom = null, activeScreenPublication = null;
    let peerNames = {}, peerAvatars = {}, waitingUsers = {}, pendingScreenRequests = {};
    let waitingCount = 0, currentHostId = null, tileOrder = ["local"], currentPage = 1, tileReactionTimers = {};
    const seenMsgIds = new Set();

    let meetingEndsAtMs = null, serverTimeOffsetMs = 0, countdownTimer = null;

    let currentViewMode = localStorage.getItem("vc_view_mode") || "grid";
    let qualityMode = localStorage.getItem("vc_quality_mode") || "auto";
    let pinnedTileId = null, lastActiveSpeakerTileId = null, lastSpeakerSwitchAt = 0, lastSpeakingKey = "";
    let lkSpeakingIds = new Set(), localMicSpeaking = false, localHoldTimer = null, localVadCtx = null, localVadTimer = null, swipeToastTimer = null;
    let participantsRenderTimer = null, dmRefreshTimer = null;

    let wantMicOnJoin = false, wantCamOnJoin = false, micEnabled = false, camEnabled = false, speakerMuted = false;
    let isSharingScreen = false, hasJoinedRoom = false, isHost = false;
    let requireScreenApproval = true, requireJoinApproval = false, autoRecordOnJoin = false, isMeetingRecording = false;
    let activeSharer = null, activeSharerName = null, screenRequestPending = false, screenShareApproved = false;

    let wbOpen = false, wbLocked = false, wbTool = "pen", wbColor = "#111827", wbHistory = [];
    let wbIsDrawing = false, wbCurrentStroke = null, wbLastPoint = null, wbLastLiveSentAt = 0;

    let isRecordingLocally = false, mediaRecorder = null, recordedChunks = [];
    let recAudioCtx = null, recAudioDest = null, recSilentOsc = null, recConnectedTrackIds = new Set();
    let recCanvas = null, recDrawTimer = null, recWorker = null, recWorkerUrl = null, recStartTime = 0, recStopPromise = null, localScreenMirrorVideo = null;

    let captionsEnabled = false, speechRecognition = null, captionClearTimer = null, lastCaptionSentAt = 0;
    let whisperWorker = null, whisperWorkerUrl = null, whisperReady = false, whisperBusy = false;
    let whisperAudioCtx = null, whisperProcessor = null, whisperSilentGain = null, whisperSources = new Map();
    let whisperBuffers = [], whisperSampleCount = 0, whisperVoiceFrames = 0, whisperActiveSpeakerLabel = "Speaker";

    let activeChatTab = "public", unreadPublic = 0, unreadPrivate = 0, localAvatar = localStorage.getItem("vc_avatar") || "";

    if (window.innerWidth > 768) chatPanel.classList.remove("hidden");
    const secondaryControlsEl = document.querySelector(".secondary-controls");
    mobileMoreBtn.onclick = (e) => {
      e.preventDefault(); e.stopPropagation();
      const willOpen = !controlsDock.classList.contains("more-open");
      if (willOpen && secondaryControlsEl) secondaryControlsEl.style.bottom = `${controlsDock.offsetHeight}px`;
      controlsDock.classList.toggle("more-open", willOpen);
    };
    document.addEventListener("click", (e) => { if (!controlsDock.contains(e.target)) controlsDock.classList.remove("more-open"); });

    async function checkOwnerAccess() {
      try { isMainHostServer = Boolean((await (await fetch("/api/owner-status")).json()).is_main_host); } catch { isMainHostServer = false; }
      scheduledMeetingsCard.classList.toggle("hidden", !isMainHostServer);
      savedRecordingsCard.classList.toggle("hidden", !isMainHostServer);
      if (isMainHostServer) { loadMeetings(); loadRecordings(); }
    }

    nameInput.value = localStorage.getItem("vc_display_name") || "User-" + clientId.slice(0, 4);
    nameInput.addEventListener("input", () => { localStorage.setItem("vc_display_name", nameInput.value.trim()); renderAvatarPreviews(); });
    function getDisplayName() { return nameInput.value.trim() || "Guest"; }

    function renderAvatarCircle(el, avatarUrl, name) {
      if (!el) return;
      if (avatarUrl) el.innerHTML = `<img src="${escapeHtml(avatarUrl)}" alt="Avatar" />`;
      else el.textContent = (name || "U").trim().slice(0, 2).toUpperCase();
    }
    function renderAvatarPreviews() {
      const name = getDisplayName();
      renderAvatarCircle(profileAvatarPreview, localAvatar, name);
      renderAvatarCircle(modalAvatarPreview, localAvatar, name);
      renderAvatarCircle(waitingAvatarPreview, localAvatar, name);
      clearAvatarBtn.classList.toggle("hidden", !localAvatar);
    }
    uploadAvatarBtn.onclick = () => avatarFileInput.click();
    avatarFileInput.onchange = (e) => {
      const file = e.target.files?.[0]; if (!file) return;
      const reader = new FileReader();
      reader.onload = (ev) => {
        const img = new Image();
        img.onload = () => {
          const canvas = document.createElement("canvas"); canvas.width = 96; canvas.height = 96;
          const size = Math.min(img.width, img.height);
          canvas.getContext("2d").drawImage(img, (img.width - size) / 2, (img.height - size) / 2, size, size, 0, 0, 96, 96);
          localAvatar = canvas.toDataURL("image/jpeg", 0.68);
          try { localStorage.setItem("vc_avatar", localAvatar); } catch {}
          renderAvatarPreviews();
        };
        img.src = ev.target.result;
      };
      reader.readAsDataURL(file); avatarFileInput.value = "";
    };
    clearAvatarBtn.onclick = () => { localAvatar = ""; localStorage.removeItem("vc_avatar"); renderAvatarPreviews(); };
    renderAvatarPreviews();
    function formatDurationLabel(mins) {
      const m = Number(mins || 0);
      if (!m || m <= 0) return "Unlimited";
      if (m >= 60 && m % 60 === 0) return `${m / 60} hr`;
      if (m > 60) return `${Math.floor(m / 60)}h ${m % 60}m`;
      return `${m} min`;
    }

    function syncMeetingCountdown(endsAtIso, serverNowIso) {
      if (serverNowIso) {
        const sMs = Date.parse(serverNowIso);
        if (!isNaN(sMs)) serverTimeOffsetMs = sMs - Date.now();
      }
      if (!endsAtIso) {
        meetingEndsAtMs = null;
        clearInterval(countdownTimer); countdownTimer = null;
        timerPill.classList.add("hidden");
        extendTimeBtn.classList.add("hidden");
        return;
      }
      const parsed = Date.parse(endsAtIso);
      if (isNaN(parsed)) return;
      meetingEndsAtMs = parsed;
      updateCountdownDisplay();
      if (!countdownTimer) countdownTimer = setInterval(updateCountdownDisplay, 500);
    }

    function updateCountdownDisplay() {
      if (!meetingEndsAtMs) { timerPill.classList.add("hidden"); extendTimeBtn.classList.add("hidden"); return; }
      const remSec = Math.max(0, Math.ceil((meetingEndsAtMs - (Date.now() + serverTimeOffsetMs)) / 1000));
      const hrs = Math.floor(remSec / 3600), mins = Math.floor((remSec % 3600) / 60), secs = remSec % 60;
      const pad = (n) => String(n).padStart(2, "0");
      timerPill.textContent = hrs > 0 ? `⏱️ ${hrs}:${pad(mins)}:${pad(secs)}` : `⏱️ ${pad(mins)}:${pad(secs)}`;
      timerPill.classList.remove("hidden", "warning", "danger");
      if (remSec <= 60) timerPill.classList.add("danger");
      else if (remSec <= 300) timerPill.classList.add("warning");
      extendTimeBtn.classList.toggle("hidden", !(isHost && currentHostId === clientId));
    }

    extendTimeBtn.onclick = () => {
      if (isHost && currentHostId === clientId) sendSignal({ type: "extend_meeting", minutes: 15 });
    };

    function showTimeWarningBanner(minutesLeft) {
      document.getElementById("time-warning-banner")?.remove();
      const card = document.createElement("div"); card.id = "time-warning-banner"; card.className = "banner-card waiting-req";
      const strictHost = Boolean(isHost && currentHostId === clientId);
      card.innerHTML = `<span class="banner-text">⏳ <b>${minutesLeft} minute${minutesLeft > 1 ? "s" : ""} remaining</b> in this meeting.</span>${strictHost ? `<div class="row" style="flex-shrink:0;"><button class="active extend-banner-btn" style="padding:0.4rem 0.7rem;">+15 min</button></div>` : ""}`;
      if (strictHost) {
        card.querySelector(".extend-banner-btn").onclick = () => { sendSignal({ type: "extend_meeting", minutes: 15 }); card.remove(); };
      }
      bannerContainer.appendChild(card);
      setTimeout(() => card.remove(), 12000);
    }

    function downloadCanvasPng(canvas, filename) {
      canvas.toBlob((blob) => {
        if (!blob) return;
        const url = URL.createObjectURL(blob), a = document.createElement("a");
        a.href = url; a.download = filename; document.body.appendChild(a); a.click(); a.remove();
        setTimeout(() => URL.revokeObjectURL(url), 4000);
      }, "image/png");
    }

    function takeTileScreenshot(tileId, personName) {
      const tile = document.getElementById(`tile-${tileId}`); if (!tile) return;
      const vid = tile.querySelector("video"), ph = tile.querySelector(".placeholder");
      if (!vid || (ph && !ph.classList.contains("hidden")) || vid.videoWidth === 0) {
        showNoticeBanner(`⚠️ Cannot take screenshot — ${personName}'s camera is off.`, "denied", 3000); return;
      }
      const canvas = document.createElement("canvas"); canvas.width = vid.videoWidth; canvas.height = vid.videoHeight;
      canvas.getContext("2d").drawImage(vid, 0, 0, canvas.width, canvas.height);
      tile.style.filter = "brightness(1.85)"; setTimeout(() => { tile.style.filter = ""; }, 160);
      const safeName = personName.replace(/[^a-z0-9_-]+/gi, "-").replace(/^-|-$/g, "") || "camera";
      downloadCanvasPng(canvas, `camera-${safeName}-${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}.png`);
      showNoticeBanner(`📸 Saved HD camera screenshot of ${personName}!`, "approved", 2500);
    }

    async function takeStageScreenshot() {
      const target = document.getElementById("stage");
      const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
      const filename = `meeting-snapshot-${currentRoomId || "meeting"}-${stamp}.png`;

      if (typeof html2canvas === "function" && target) {
        const videoCanvases = new Map(), videos = Array.from(target.querySelectorAll("video"));
        videos.forEach((vid, idx) => {
          if (vid.videoWidth > 0 && vid.videoHeight > 0 && vid.offsetParent !== null) {
            const cw = vid.clientWidth || vid.videoWidth, ch = vid.clientHeight || vid.videoHeight;
            const c = document.createElement("canvas"); c.width = cw; c.height = ch;
            const tctx = c.getContext("2d"), vw = vid.videoWidth, vh = vid.videoHeight;
            const scale = (vid.id === "screen-stage-video") ? Math.min(cw / vw, ch / vh) : Math.max(cw / vw, ch / vh);
            const dw = vw * scale, dh = vh * scale;
            tctx.fillStyle = "#000"; tctx.fillRect(0, 0, cw, ch);
            tctx.drawImage(vid, (cw - dw) / 2, (ch - dh) / 2, dw, dh);
            vid.dataset.snapIdx = String(idx); videoCanvases.set(String(idx), c);
          }
        });

        try {
          const captured = await html2canvas(target, {
            backgroundColor: "#0f1115",
            scale: Math.min(window.devicePixelRatio || 1, 2),
            useCORS: true,
            logging: false,
            onclone: (clonedDoc) => {
              ["mobile-view-bar", "stage-footer", "caption-bar", "view-swipe-toast", "reaction-layer"].forEach((id) => {
                const el = clonedDoc.getElementById(id); if (el) el.style.display = "none";
              });
              clonedDoc.querySelectorAll(".tile-actions").forEach((el) => { el.style.display = "none"; });
              clonedDoc.querySelectorAll("video[data-snap-idx]").forEach((clonedVid) => {
                const c = videoCanvases.get(clonedVid.dataset.snapIdx);
                if (c && clonedVid.parentNode) {
                  c.style.cssText = "position:relative; z-index:0; width:100%; height:100%; display:block; background:#000;";
                  clonedVid.parentNode.replaceChild(c, clonedVid);
                }
              });
            }
          });
          videos.forEach((v) => delete v.dataset.snapIdx);

          captured.toBlob(async (blob) => {
            if (!blob) return;
            let copied = false;
            if (navigator.clipboard?.write && typeof ClipboardItem !== "undefined") {
              try { await navigator.clipboard.write([new ClipboardItem({ "image/png": blob })]); copied = true; } catch {}
            }
            const url = URL.createObjectURL(blob), a = document.createElement("a");
            a.href = url; a.download = filename; document.body.appendChild(a); a.click(); a.remove();
            setTimeout(() => URL.revokeObjectURL(url), 4000);
            showNoticeBanner(copied ? "📸 Snapshot saved & copied to clipboard!" : "📸 Snapshot saved to Downloads!", "approved", 3000);
          }, "image/png");
          return;
        } catch { videos.forEach((v) => delete v.dataset.snapIdx); }
      }
      showNoticeBanner("⚠️ Could not capture snapshot.", "denied", 3000);
    }
    snapshotBtn.onclick = () => { controlsDock.classList.remove("more-open"); takeStageScreenshot(); };

    function canDrawOnWhiteboard() { return !wbLocked || isHost; }
    function updateWhiteboardUI() {
      whiteboardStage.classList.toggle("hidden", !wbOpen); wbToggleBtn.classList.toggle("active", wbOpen);
      wbToggleBtn.textContent = wbOpen ? "🎨 Close Board" : "🎨 Whiteboard";
      const writable = canDrawOnWhiteboard();
      wbCanvas.style.pointerEvents = writable ? "auto" : "none"; wbCanvas.style.cursor = writable ? "crosshair" : "not-allowed";
      wbLockedPill.classList.toggle("hidden", !wbLocked || isHost); wbClearBtn.disabled = !writable; wbUndoBtn.disabled = !writable;
      wbLockBtn.classList.toggle("hidden", !isHost);
      if (isHost) { wbLockBtn.textContent = wbLocked ? "🔒 Host Only Draws" : "🔓 Everyone Draws"; wbLockBtn.classList.toggle("warning", wbLocked); wbLockBtn.classList.toggle("secondary", !wbLocked); }
    }
    function drawSingleStroke(ctx, stroke) {
      const w = wbCanvas.width, h = wbCanvas.height; ctx.save(); ctx.lineCap = "round"; ctx.lineJoin = "round";
      const tool = stroke.tool || "pen", baseSize = Number(stroke.size || 6);
      if (tool === "eraser") { ctx.strokeStyle = "#ffffff"; ctx.lineWidth = baseSize * 4; }
      else if (tool === "highlighter") { ctx.strokeStyle = stroke.color || "#111827"; ctx.globalAlpha = 0.35; ctx.lineWidth = baseSize * 3.5; }
      else { ctx.strokeStyle = stroke.color || "#111827"; ctx.fillStyle = stroke.color || "#111827"; ctx.lineWidth = baseSize; }
      if (tool === "pen" || tool === "highlighter" || tool === "eraser") {
        const pts = stroke.points || [];
        if (pts.length === 1) { ctx.beginPath(); ctx.arc(pts[0][0] * w, pts[0][1] * h, ctx.lineWidth / 2, 0, Math.PI * 2); ctx.fillStyle = tool === "eraser" ? "#ffffff" : ctx.strokeStyle; ctx.fill(); }
        else if (pts.length > 1) { ctx.beginPath(); ctx.moveTo(pts[0][0] * w, pts[0][1] * h); for (let i = 1; i < pts.length; i++) ctx.lineTo(pts[i][0] * w, pts[i][1] * h); ctx.stroke(); }
      } else if (tool === "line") { ctx.beginPath(); ctx.moveTo(stroke.x1 * w, stroke.y1 * h); ctx.lineTo(stroke.x2 * w, stroke.y2 * h); ctx.stroke(); }
      else if (tool === "rect") { ctx.strokeRect(Math.min(stroke.x1, stroke.x2) * w, Math.min(stroke.y1, stroke.y2) * h, Math.abs(stroke.x2 - stroke.x1) * w, Math.abs(stroke.y2 - stroke.y1) * h); }
      else if (tool === "circle") { ctx.beginPath(); ctx.ellipse(((stroke.x1 + stroke.x2) / 2) * w, ((stroke.y1 + stroke.y2) / 2) * h, Math.max(1, Math.abs(stroke.x2 - stroke.x1) * w / 2), Math.max(1, Math.abs(stroke.y2 - stroke.y1) * h / 2), 0, 0, Math.PI * 2); ctx.stroke(); }
      else if (tool === "text" && stroke.text) { ctx.font = `600 ${Math.max(20, baseSize * 5)}px system-ui, sans-serif`; ctx.textBaseline = "top"; ctx.fillText(stroke.text, stroke.x * w, stroke.y * h); }
      ctx.restore();
    }
    function redrawWhiteboard() { wbCtx.fillStyle = "#ffffff"; wbCtx.fillRect(0, 0, wbCanvas.width, wbCanvas.height); for (const s of wbHistory) drawSingleStroke(wbCtx, s); }
    function getNormalizedPoint(e) { const r = wbCanvas.getBoundingClientRect(); return [Math.max(0, Math.min(1, (e.clientX - r.left) / r.width)), Math.max(0, Math.min(1, (e.clientY - r.top) / r.height))]; }
    document.querySelectorAll(".wb-tool").forEach(btn => { btn.onclick = () => { wbTool = btn.dataset.tool; document.querySelectorAll(".wb-tool").forEach(b => b.classList.remove("wb-tool-active")); btn.classList.add("wb-tool-active"); }; });
    document.querySelectorAll(".wb-swatch").forEach(btn => { btn.onclick = () => { wbColor = btn.dataset.color; document.querySelectorAll(".wb-swatch").forEach(b => b.classList.remove("active-swatch")); btn.classList.add("active-swatch"); if (wbTool === "eraser") document.querySelector('.wb-tool[data-tool="pen"]')?.click(); }; });
    wbCanvas.addEventListener("pointerdown", (e) => {
      if (!canDrawOnWhiteboard()) return; e.preventDefault();
      const [nx, ny] = getNormalizedPoint(e), size = Number(wbSizeSelect.value || 6);
      if (wbTool === "text") {
        const text = prompt("Enter text for whiteboard:");
        if (text && text.trim()) { const stroke = { id: `${clientId}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`, sender: clientId, tool: "text", color: wbColor, size, x: nx, y: ny, text: text.trim().slice(0, 200) }; wbHistory.push(stroke); redrawWhiteboard(); sendSignal({ type: "wb_commit", stroke }); }
        return;
      }
      wbCanvas.setPointerCapture?.(e.pointerId); wbIsDrawing = true; wbLastPoint = [nx, ny];
      const strokeId = `${clientId}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
      if (wbTool === "pen" || wbTool === "highlighter" || wbTool === "eraser") { wbCurrentStroke = { id: strokeId, sender: clientId, tool: wbTool, color: wbColor, size, points: [[nx, ny]] }; drawSingleStroke(wbCtx, wbCurrentStroke); }
      else { wbCurrentStroke = { id: strokeId, sender: clientId, tool: wbTool, color: wbColor, size, x1: nx, y1: ny, x2: nx, y2: ny }; }
    });
    wbCanvas.addEventListener("pointermove", (e) => {
      if (!wbIsDrawing || !wbCurrentStroke) return; e.preventDefault();
      const [nx, ny] = getNormalizedPoint(e);
      if (wbCurrentStroke.points) {
        const prev = wbLastPoint; wbCurrentStroke.points.push([nx, ny]);
        drawSingleStroke(wbCtx, { tool: wbCurrentStroke.tool, color: wbCurrentStroke.color, size: wbCurrentStroke.size, points: [prev, [nx, ny]] });
        wbLastPoint = [nx, ny];
        const now = Date.now();
        if (now - wbLastLiveSentAt > 35) { wbLastLiveSentAt = now; sendSignal({ type: "wb_live", tool: wbCurrentStroke.tool, color: wbCurrentStroke.color, size: wbCurrentStroke.size, x1: prev[0], y1: prev[1], x2: nx, y2: ny }); }
      } else { wbCurrentStroke.x2 = nx; wbCurrentStroke.y2 = ny; wbPreviewCtx.clearRect(0, 0, wbPreviewCanvas.width, wbPreviewCanvas.height); drawSingleStroke(wbPreviewCtx, wbCurrentStroke); }
    });
    function finishWhiteboardStroke() {
      if (!wbIsDrawing || !wbCurrentStroke) return;
      wbIsDrawing = false; wbPreviewCtx.clearRect(0, 0, wbPreviewCanvas.width, wbPreviewCanvas.height);
      wbHistory.push(wbCurrentStroke); redrawWhiteboard(); sendSignal({ type: "wb_commit", stroke: wbCurrentStroke });
      wbCurrentStroke = null; wbLastPoint = null;
    }
    wbCanvas.addEventListener("pointerup", finishWhiteboardStroke); wbCanvas.addEventListener("pointercancel", finishWhiteboardStroke);
    wbUndoBtn.onclick = () => { if (!canDrawOnWhiteboard()) return; for (let i = wbHistory.length - 1; i >= 0; i--) { if (wbHistory[i].sender === clientId || isHost) { const [rm] = wbHistory.splice(i, 1); redrawWhiteboard(); sendSignal({ type: "wb_undo", stroke_id: rm.id }); break; } } };
    wbClearBtn.onclick = () => { if (!canDrawOnWhiteboard()) return; wbHistory = []; redrawWhiteboard(); sendSignal({ type: "wb_clear" }); };
    wbSaveBtn.onclick = () => { const a = document.createElement("a"); a.href = wbCanvas.toDataURL("image/png"); a.download = `whiteboard-${currentRoomId || "meeting"}.png`; a.click(); };
    wbLockBtn.onclick = () => { if (isHost) sendSignal({ type: "wb_lock", locked: !wbLocked }); };
    wbToggleBtn.onclick = () => { wbOpen = !wbOpen; controlsDock.classList.remove("more-open"); updateWhiteboardUI(); if (wbOpen) redrawWhiteboard(); sendSignal({ type: "wb_toggle", open: wbOpen }); };
    wbCloseBtn.onclick = async () => { if (document.fullscreenElement === whiteboardStage || document.webkitFullscreenElement === whiteboardStage) { try { if (document.exitFullscreen) await document.exitFullscreen(); else if (document.webkitExitFullscreen) await document.webkitExitFullscreen(); } catch {} } wbOpen = false; updateWhiteboardUI(); sendSignal({ type: "wb_toggle", open: false }); };
    wbFullscreenBtn.onclick = async () => { const fsEl = document.fullscreenElement || document.webkitFullscreenElement; try { if (fsEl !== whiteboardStage) { if (whiteboardStage.requestFullscreen) await whiteboardStage.requestFullscreen(); else if (whiteboardStage.webkitRequestFullscreen) await whiteboardStage.webkitRequestFullscreen(); } else { if (document.exitFullscreen) await document.exitFullscreen(); else if (document.webkitExitFullscreen) await document.webkitExitFullscreen(); } } catch {} };
    redrawWhiteboard();

    function startLocalVoiceMonitor(mediaStreamTrack) {
      stopLocalVoiceMonitor(); if (!mediaStreamTrack) return;
      try {
        const AudioCtx = window.AudioContext || window.webkitAudioContext; localVadCtx = new AudioCtx();
        if (localVadCtx.state === "suspended") localVadCtx.resume().catch(() => {});
        const src = localVadCtx.createMediaStreamSource(new MediaStream([mediaStreamTrack]));
        const analyser = localVadCtx.createAnalyser(); analyser.fftSize = 256; src.connect(analyser);
        const data = new Uint8Array(analyser.frequencyBinCount);
        localVadTimer = setInterval(() => {
          if (!micEnabled) { if (localMicSpeaking) { localMicSpeaking = false; updateSpeakingUI(); } return; }
          analyser.getByteFrequencyData(data);
          let sum = 0; for (let i = 0; i < data.length; i++) sum += data[i];
          if (sum / data.length > 12) { clearTimeout(localHoldTimer); localHoldTimer = null; if (!localMicSpeaking) { localMicSpeaking = true; updateSpeakingUI(); } }
          else if (localMicSpeaking && !localHoldTimer) { localHoldTimer = setTimeout(() => { localHoldTimer = null; localMicSpeaking = false; updateSpeakingUI(); }, 450); }
        }, 150);
      } catch {}
    }
    function stopLocalVoiceMonitor() {
      if (localVadTimer) { clearInterval(localVadTimer); localVadTimer = null; }
      clearTimeout(localHoldTimer); localHoldTimer = null;
      if (localVadCtx) { localVadCtx.close().catch(() => {}); localVadCtx = null; }
      localMicSpeaking = false; updateSpeakingUI();
    }
    function updateSpeakingUI() {
      const speakingSet = new Set(lkSpeakingIds);
      if (localMicSpeaking && micEnabled) speakingSet.add("local");
      const key = Array.from(speakingSet).sort().join(",");
      if (key === lastSpeakingKey) return;
      lastSpeakingKey = key;
      for (const id of speakingSet) { if (id !== "local") { lastActiveSpeakerTileId = id; break; } }
      if (!lastActiveSpeakerTileId && speakingSet.has("local")) lastActiveSpeakerTileId = "local";
      document.querySelectorAll("#video-grid .video-tile").forEach(tile => tile.classList.toggle("speaking", speakingSet.has(tile.id.replace("tile-", ""))));
      if (!participantsPanel.classList.contains("hidden")) {
        document.querySelectorAll("#participants-list .participant-row[data-pid]").forEach(row => {
          const pid = row.dataset.pid; row.classList.toggle("speaking", speakingSet.has(pid === clientId ? "local" : pid));
        });
      }
      if ((currentViewMode === "speaker" || currentViewMode === "sidebar") && !pinnedTileId) {
        const now = Date.now();
        if (now - lastSpeakerSwitchAt > 1200) { lastSpeakerSwitchAt = now; updateFeaturedTile(); }
      }
    }

    function applyQualityMode(mode) {
      qualityMode = mode === "saver" ? "saver" : "auto"; qualityModeSelect.value = qualityMode;
      try { localStorage.setItem("vc_quality_mode", qualityMode); } catch {}
      applySubscriptionEfficiency();
    }
    qualityModeSelect.value = qualityMode;
    qualityModeSelect.onchange = () => { applyQualityMode(qualityModeSelect.value); controlsDock.classList.remove("more-open"); };

    function applySubscriptionEfficiency() {
      if (!lkRoom) return;
      const pageSize = getPageSize(), start = (currentPage - 1) * pageSize, end = start + pageSize, visibleIds = new Set();
      tileOrder.forEach((id, idx) => { const tile = document.getElementById(`tile-${id}`); if ((idx >= start && idx < end) || (tile && tile.classList.contains("featured-tile"))) visibleIds.add(id); });
      for (const participant of lkRoom.remoteParticipants.values()) {
        const pid = participant.identity, isVisible = visibleIds.has(pid);
        const isFeatured = Boolean(document.getElementById(`tile-${pid}`)?.classList.contains("featured-tile"));
        for (const pub of participant.trackPublications.values()) {
          if (pub.source === LivekitClient.Track.Source.Camera) {
            try {
              pub.setEnabled?.(isVisible);
              if (isVisible) {
                if (qualityMode === "saver") pub.setVideoQuality?.(LivekitClient.VideoQuality.LOW);
                else if (isFeatured || tileOrder.length <= 4) pub.setVideoQuality?.(LivekitClient.VideoQuality.HIGH);
                else pub.setVideoQuality?.(LivekitClient.VideoQuality.MEDIUM);
              }
            } catch {}
          }
        }
      }
    }

    function pruneGhostTiles() {
      const validIds = new Set(Object.keys(peerNames));
      if (lkRoom) { for (const p of lkRoom.remoteParticipants.values()) validIds.add(p.identity); }
      for (const tid of [...tileOrder]) { if (tid !== "local" && !validIds.has(tid)) removeVideoTile(tid); }
    }

    function getPageSize() { return currentViewMode === "compact" ? 24 : 12; }
    function showSwipeViewToast(mode) {
      viewSwipeToast.textContent = VIEW_LABELS[mode] || mode; viewSwipeToast.classList.remove("hidden");
      clearTimeout(swipeToastTimer); swipeToastTimer = setTimeout(() => viewSwipeToast.classList.add("hidden"), 1400);
    }
    function applyViewMode(mode, swipeDirection = null) {
      if (!VIEW_MODES.includes(mode)) mode = "grid";
      currentViewMode = mode; viewModeSelect.value = mode;
      try { localStorage.setItem("vc_view_mode", mode); } catch {}
      document.querySelectorAll("#mobile-view-bar .m-view-pill").forEach(p => p.classList.toggle("active-pill", p.dataset.mode === mode));
      videoGrid.classList.remove("mode-grid", "mode-speaker", "mode-sidebar", "mode-compact", "swipe-next", "swipe-prev");
      videoGrid.classList.add(`mode-${mode}`); videoGrid.classList.toggle("single-tile", tileOrder.length <= 1);
      if (swipeDirection === "next" || swipeDirection === "prev") { void videoGrid.offsetWidth; videoGrid.classList.add(swipeDirection === "next" ? "swipe-next" : "swipe-prev"); showSwipeViewToast(mode); }
      updateFeaturedTile(); renderPagination();
    }
    videoGrid.addEventListener("animationend", (e) => { if (e.target === videoGrid) videoGrid.classList.remove("swipe-next", "swipe-prev"); });

    let touchStartX = 0, touchStartY = 0, touchStartTime = 0, trackingSwipe = false;
    videoGrid.addEventListener("touchstart", (e) => {
      if (e.touches.length !== 1 || e.target.closest(".pin-btn") || e.target.closest(".snap-btn")) { trackingSwipe = false; return; }
      touchStartX = e.touches[0].clientX; touchStartY = e.touches[0].clientY; touchStartTime = Date.now(); trackingSwipe = true;
    }, { passive: true });
    videoGrid.addEventListener("touchend", (e) => {
      if (!trackingSwipe || !e.changedTouches.length) return; trackingSwipe = false;
      const dx = e.changedTouches[0].clientX - touchStartX, dy = e.changedTouches[0].clientY - touchStartY;
      if (Date.now() - touchStartTime < 650 && Math.abs(dx) > 55 && Math.abs(dx) > Math.abs(dy) * 1.45) {
        const idx = VIEW_MODES.indexOf(currentViewMode);
        applyViewMode(VIEW_MODES[dx < 0 ? (idx + 1) % VIEW_MODES.length : (idx - 1 + VIEW_MODES.length) % VIEW_MODES.length], dx < 0 ? "next" : "prev");
      }
    }, { passive: true });
    document.querySelectorAll("#mobile-view-bar .m-view-pill").forEach(pill => {
      pill.onclick = () => { const tm = pill.dataset.mode; if (tm !== currentViewMode) applyViewMode(tm, VIEW_MODES.indexOf(tm) > VIEW_MODES.indexOf(currentViewMode) ? "next" : "prev"); };
    });

    function updateFeaturedTile() {
      document.querySelectorAll("#video-grid .video-tile").forEach(tile => {
        tile.classList.remove("featured-tile");
        const pb = tile.querySelector(".pin-btn");
        if (pb) { const isP = tile.id === `tile-${pinnedTileId}`; pb.classList.toggle("pinned", isP); pb.textContent = isP ? "📌 Pinned" : "📌 Pin"; }
      });
      if (currentViewMode === "speaker" || currentViewMode === "sidebar") {
        let targetId = (pinnedTileId && document.getElementById(`tile-${pinnedTileId}`)) ? pinnedTileId
          : (lastActiveSpeakerTileId && document.getElementById(`tile-${lastActiveSpeakerTileId}`)) ? lastActiveSpeakerTileId
          : (tileOrder.find(id => id !== "local") || "local");
        const tt = document.getElementById(`tile-${targetId}`);
        if (tt) { tt.classList.add("featured-tile"); tt.classList.remove("page-hidden"); }
      }
      applySubscriptionEfficiency();
    }
    viewModeSelect.value = currentViewMode;
    viewModeSelect.onchange = () => { applyViewMode(viewModeSelect.value); controlsDock.classList.remove("more-open"); };

    function playChime(notes) {
      if (speakerMuted) return;
      try {
        const AudioCtx = window.AudioContext || window.webkitAudioContext; if (!AudioCtx) return;
        const ctx = new AudioCtx(); if (ctx.state === "suspended") ctx.resume().catch(() => {});
        const now = ctx.currentTime;
        notes.forEach(([freq, dur], idx) => {
          const osc = ctx.createOscillator(), gain = ctx.createGain();
          osc.type = "sine"; osc.frequency.setValueAtTime(freq, now + idx * 0.11);
          gain.gain.setValueAtTime(0.001, now + idx * 0.11); gain.gain.exponentialRampToValueAtTime(0.14, now + idx * 0.11 + 0.02); gain.gain.exponentialRampToValueAtTime(0.001, now + idx * 0.11 + dur);
          osc.connect(gain); gain.connect(ctx.destination); osc.start(now + idx * 0.11); osc.stop(now + idx * 0.11 + dur + 0.02);
        });
        setTimeout(() => ctx.close().catch(() => {}), 800);
      } catch {}
    }
    function playJoinSound() { playChime([[523.25, 0.16], [659.25, 0.24]]); }
    function playWaitingKnockSound() { playChime([[440, 0.14], [554.37, 0.14], [659.25, 0.25]]); }

    function getMeetingUrl(roomId) { return `${location.origin}/room/${encodeURIComponent(roomId)}`; }
    function escapeHtml(str) { return String(str).replace(/[&<>"']/g, s => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[s])); }
    async function copyText(text, btn) {
      try { await navigator.clipboard.writeText(text); if (btn) { const o = btn.innerHTML; btn.textContent = "Copied!"; setTimeout(() => btn.innerHTML = o, 1500); } }
      catch { prompt("Copy this meeting link:", text); }
    }
    function formatDateTime(iso) { if (!iso) return "Instant / Unscheduled"; const d = new Date(iso); return isNaN(d) ? iso : d.toLocaleString(); }
    function rememberMsgId(id) {
      if (!id) return false; if (seenMsgIds.has(id)) return true; seenMsgIds.add(id);
      if (seenMsgIds.size > 400) seenMsgIds.delete(seenMsgIds.values().next().value);
      return false;
    }

    function getRemoteMediaStatus(pid) {
      if (!lkRoom) return { mic: false, cam: false };
      const p = lkRoom.remoteParticipants.get(pid); if (!p) return { mic: false, cam: false };
      const micPub = p.getTrackPublication(LivekitClient.Track.Source.Microphone), camPub = p.getTrackPublication(LivekitClient.Track.Source.Camera);
      return { mic: Boolean(micPub && !micPub.isMuted), cam: Boolean(camPub && !camPub.isMuted) };
    }

    function syncPeersFromLiveKit() {
      if (!lkRoom) return;
      let changed = false;
      for (const participant of lkRoom.remoteParticipants.values()) {
        const pid = participant.identity; if (!pid || pid === clientId) continue;
        const pName = participant.name || peerNames[pid] || `User ${pid}`;
        if (peerNames[pid] !== pName) { peerNames[pid] = pName; changed = true; }
        if (!document.getElementById(`tile-${pid}`)) { ensureVideoTile(pid, pName, true, peerAvatars[pid] || ""); changed = true; }
      }
      if (changed) scheduleDmRefresh();
    }

    function updateParticipantCounters() {
      const inMeetingCount = Object.keys(peerNames).length + 1;
      participantCountEl.textContent = `👥 ${inMeetingCount} / ${maxUsersInRoom}`;
      participantsBtnCount.textContent = inMeetingCount;
      participantsPanelTitle.textContent = `Participants (${inMeetingCount} / ${maxUsersInRoom})`;
      const effectiveWaiting = (isHost ? Object.keys(waitingUsers).length : waitingCount) + (isHost ? Object.keys(pendingScreenRequests).length : 0);
      joiningCountPill.classList.toggle("hidden", effectiveWaiting === 0);
      joiningCountPill.textContent = `⏳ ${effectiveWaiting} pending`;
      joiningBtnBadge.classList.toggle("hidden", effectiveWaiting === 0);
      joiningBtnBadge.textContent = `${effectiveWaiting}`;
    }

    function sendScreenDecision(requesterId, approved) {
      delete pendingScreenRequests[requesterId];
      document.getElementById(`req-${requesterId}`)?.remove();
      sendSignal({ type: "screen_share_decision", target: requesterId, approved });
      sendRoomData({ type: approved ? "screen_share_approved" : "screen_share_denied", reason: approved ? "" : "The meeting host declined your screen share request." }, requesterId);
      scheduleParticipantsRender();
    }

    function scheduleParticipantsRender() {
      updateParticipantCounters();
      if (participantsPanel.classList.contains("hidden") || participantsRenderTimer) return;
      participantsRenderTimer = setTimeout(() => { participantsRenderTimer = null; renderParticipantsList(); }, 120);
    }

    function scheduleDmRefresh() {
      updateParticipantCounters();
      if (dmRefreshTimer) return;
      dmRefreshTimer = setTimeout(() => { dmRefreshTimer = null; refreshDmDropdown(); }, 120);
    }

    function renderParticipantsList() {
      updateParticipantCounters();
      if (participantsPanel.classList.contains("hidden")) return;
      const filter = participantSearch.value.trim().toLowerCase();
      participantsList.innerHTML = "";
      const frag = document.createDocumentFragment();

      const shareReqEntries = Object.values(pendingScreenRequests);
      if (isHost && shareReqEntries.length > 0) {
        const reqHeader = document.createElement("div"); reqHeader.className = "p-section-title"; reqHeader.innerHTML = `<span>🖥️ Screen Share Requests (${shareReqEntries.length})</span>`;
        frag.appendChild(reqHeader);
        for (const req of shareReqEntries) {
          if (filter && !req.name.toLowerCase().includes(filter)) continue;
          const row = document.createElement("div"); row.className = "participant-row"; row.style.borderColor = "#3b82f6";
          const left = document.createElement("div"); left.className = "participant-left";
          const av = document.createElement("div"); av.className = "avatar-circle small"; renderAvatarCircle(av, peerAvatars[req.id] || "", req.name);
          const nw = document.createElement("div"); nw.className = "participant-name-wrap";
          nw.innerHTML = `<span class="participant-name">${escapeHtml(req.name)}</span><span class="participant-tags"><span class="mini-tag" style="background:#1d4ed8;color:#fff;">Wants to Share</span></span>`;
          left.appendChild(av); left.appendChild(nw);
          const right = document.createElement("div"); right.className = "participant-right";
          right.innerHTML = `<button class="active p-allow-share" style="padding:0.28rem 0.55rem;font-size:0.75rem;">Allow</button><button class="danger p-deny-share" style="padding:0.28rem 0.55rem;font-size:0.75rem;">Deny</button>`;
          right.querySelector(".p-allow-share").onclick = () => sendScreenDecision(req.id, true);
          right.querySelector(".p-deny-share").onclick = () => sendScreenDecision(req.id, false);
          row.appendChild(left); row.appendChild(right); frag.appendChild(row);
        }
      }

      const waitingEntries = Object.values(waitingUsers), effectiveWaiting = isHost ? waitingEntries.length : waitingCount;
      if (effectiveWaiting > 0) {
        const waitHeader = document.createElement("div"); waitHeader.className = "p-section-title"; waitHeader.innerHTML = `<span>⏳ Joining / Waiting Room (${effectiveWaiting})</span>`;
        if (isHost && waitingEntries.length > 0) {
          const admitAllBtn = document.createElement("button"); admitAllBtn.className = "active"; admitAllBtn.style.cssText = "padding: 0.2rem 0.55rem; font-size: 0.72rem;"; admitAllBtn.textContent = "Admit All";
          admitAllBtn.onclick = () => { waitingEntries.forEach(u => document.getElementById(`wait-${u.id}`)?.remove()); waitingUsers = {}; waitingCount = 0; sendSignal({ type: "admit_all" }); renderParticipantsList(); };
          waitHeader.appendChild(admitAllBtn);
        }
        frag.appendChild(waitHeader);
        if (isHost) {
          for (const u of waitingEntries) {
            if (filter && !u.name.toLowerCase().includes(filter)) continue;
            const row = document.createElement("div"); row.className = "participant-row"; row.style.borderColor = "#d97706";
            const left = document.createElement("div"); left.className = "participant-left";
            const av = document.createElement("div"); av.className = "avatar-circle small"; renderAvatarCircle(av, u.avatar, u.name);
            const nw = document.createElement("div"); nw.className = "participant-name-wrap";
            nw.innerHTML = `<span class="participant-name">${escapeHtml(u.name)}</span><span class="participant-tags"><span class="mini-tag" style="background:#b45309;color:#fff;">Waiting</span></span>`;
            left.appendChild(av); left.appendChild(nw);
            const right = document.createElement("div"); right.className = "participant-right";
            right.innerHTML = `<button class="active p-admit-btn" style="padding:0.28rem 0.55rem;font-size:0.75rem;">Admit</button><button class="danger p-decline-btn" style="padding:0.28rem 0.55rem;font-size:0.75rem;">✕</button>`;
            right.querySelector(".p-admit-btn").onclick = () => { delete waitingUsers[u.id]; document.getElementById(`wait-${u.id}`)?.remove(); sendSignal({ type: "join_decision", target: u.id, approved: true }); renderParticipantsList(); };
            right.querySelector(".p-decline-btn").onclick = () => { delete waitingUsers[u.id]; document.getElementById(`wait-${u.id}`)?.remove(); sendSignal({ type: "join_decision", target: u.id, approved: false }); renderParticipantsList(); };
            row.appendChild(left); row.appendChild(right); frag.appendChild(row);
          }
        } else {
          const infoBox = document.createElement("div"); infoBox.className = "waiting-banner-box";
          infoBox.textContent = `${effectiveWaiting} participant(s) are currently in the waiting room waiting for host approval.`;
          frag.appendChild(infoBox);
        }
      }

      const allParticipants = [
        { id: clientId, name: getDisplayName(), avatar: localAvatar, isSelf: true, isHost: isHost && currentHostId === clientId, isPresenting: isSharingScreen, mic: micEnabled, cam: camEnabled },
        ...Object.entries(peerNames).map(([id, name]) => { const s = getRemoteMediaStatus(id); return { id, name, avatar: peerAvatars[id] || "", isSelf: false, isHost: id === currentHostId, isPresenting: id === activeSharer, mic: s.mic, cam: s.cam }; })
      ];
      const inHeader = document.createElement("div"); inHeader.className = "p-section-title"; inHeader.textContent = `In Meeting (${allParticipants.length})`;
      frag.appendChild(inHeader);

      const speakingSet = new Set(lkSpeakingIds); if (localMicSpeaking && micEnabled) speakingSet.add("local");
      for (const p of allParticipants) {
        if (filter && !p.name.toLowerCase().includes(filter)) continue;
        const row = document.createElement("div"); row.className = `participant-row${speakingSet.has(p.isSelf ? "local" : p.id) ? " speaking" : ""}`; row.dataset.pid = p.id;
        const left = document.createElement("div"); left.className = "participant-left";
        const av = document.createElement("div"); av.className = "avatar-circle small"; renderAvatarCircle(av, p.avatar, p.name);
        const tags = [];
        if (p.isSelf) tags.push(`<span class="mini-tag">You</span>`);
        if (p.isHost) tags.push(`<span class="mini-tag host">Host</span>`);
        if (p.isPresenting) tags.push(`<span class="mini-tag screen">Presenting</span>`);
        const nw = document.createElement("div"); nw.className = "participant-name-wrap";
        nw.innerHTML = `<span class="participant-name">${escapeHtml(p.name)}</span>${tags.length ? `<div class="participant-tags">${tags.join("")}</div>` : ""}`;
        left.appendChild(av); left.appendChild(nw);
        const right = document.createElement("div"); right.className = "participant-right";
        right.innerHTML = `<span>${p.mic ? "🎤" : "🔇"}</span><span>${p.cam ? "📷" : "🚫"}</span>`;
        if (!p.isSelf) {
          const dmBtn = document.createElement("button"); dmBtn.className = "secondary"; dmBtn.style.cssText = "padding:0.25rem 0.5rem; font-size:0.75rem;"; dmBtn.textContent = "DM";
          dmBtn.onclick = () => { participantsPanel.classList.add("hidden"); chatPanel.classList.remove("hidden"); switchChatTab("private"); dmTarget.value = p.id; chatInput.focus(); };
          right.appendChild(dmBtn);
        }
        row.appendChild(left); row.appendChild(right); frag.appendChild(row);
      }
      participantsList.appendChild(frag);
    }

    participantSearch.addEventListener("input", renderParticipantsList);
    toggleParticipantsBtn.onclick = () => {
      const opening = participantsPanel.classList.contains("hidden");
      participantsPanel.classList.toggle("hidden", !opening);
      if (opening) { chatPanel.classList.add("hidden"); controlsDock.classList.remove("more-open"); syncPeersFromLiveKit(); renderParticipantsList(); }
      updateUnreadBadges();
    };
    closeParticipantsBtn.onclick = () => { participantsPanel.classList.add("hidden"); };
    closeChatBtn.onclick = () => { chatPanel.classList.add("hidden"); updateUnreadBadges(); };

    function openRecordingPlayer(title, url) { playerTitle.textContent = title; playerVideo.src = url; playerModal.classList.remove("hidden"); }
    playerCloseBtn.onclick = () => { playerVideo.pause(); playerVideo.removeAttribute("src"); playerVideo.load(); playerModal.classList.add("hidden"); };

    function isScreenFullscreen() { const fsEl = document.fullscreenElement || document.webkitFullscreenElement; return fsEl === screenStage || fsEl === screenStageVideo; }
    function applyHighQualityScreenShare() {
      if (!activeScreenPublication) return;
      try {
        activeScreenPublication.setVideoQuality?.(LivekitClient.VideoQuality.HIGH);
        const dpr = window.devicePixelRatio || 1;
        activeScreenPublication.setVideoDimensions?.({ width: isScreenFullscreen() ? Math.max(1920, Math.round(window.screen.width * dpr)) : 1920, height: isScreenFullscreen() ? Math.max(1080, Math.round(window.screen.height * dpr)) : 1080 });
      } catch {}
    }
    async function toggleScreenFullscreen() {
      if (screenStageVideo.classList.contains("hidden")) return;
      try {
        if (!isScreenFullscreen()) {
          if (screenStage.requestFullscreen) await screenStage.requestFullscreen();
          else if (screenStage.webkitRequestFullscreen) await screenStage.webkitRequestFullscreen();
          else if (screenStageVideo.webkitEnterFullscreen) screenStageVideo.webkitEnterFullscreen();
        } else {
          if (document.exitFullscreen) await document.exitFullscreen();
          else if (document.webkitExitFullscreen) await document.webkitExitFullscreen();
        }
      } catch { if (screenStageVideo.webkitEnterFullscreen) try { screenStageVideo.webkitEnterFullscreen(); } catch {} }
    }
    async function exitScreenFullscreenIfActive() { if (isScreenFullscreen()) { try { if (document.exitFullscreen) await document.exitFullscreen(); else if (document.webkitExitFullscreen) await document.webkitExitFullscreen(); } catch {} } }
    function onFullscreenChange() {
      screenFullscreenBtn.textContent = isScreenFullscreen() ? "✕ Exit Fullscreen" : "⛶ Fullscreen";
      applyHighQualityScreenShare();
      wbFullscreenBtn.textContent = (document.fullscreenElement || document.webkitFullscreenElement) === whiteboardStage ? "✕ Exit" : "⛶ Fullscreen";
    }
    screenFullscreenBtn.onclick = toggleScreenFullscreen; screenStageVideo.ondblclick = toggleScreenFullscreen;
    document.addEventListener("fullscreenchange", onFullscreenChange); document.addEventListener("webkitfullscreenchange", onFullscreenChange);

    function showNoticeBanner(text, variant = "", durationMs = 4500) {
      const card = document.createElement("div"); card.className = `banner-card ${variant}`;
      card.innerHTML = `<span class="banner-text">${escapeHtml(text)}</span>`; bannerContainer.appendChild(card);
      if (durationMs > 0) setTimeout(() => card.remove(), durationMs);
      return card;
    }

    function showWaitingUserBanner(user) {
      document.getElementById(`wait-${user.id}`)?.remove();
      const card = document.createElement("div"); card.id = `wait-${user.id}`; card.className = "banner-card waiting-req";
      const textSpan = document.createElement("span"); textSpan.className = "banner-text";
      const avatarEl = document.createElement("div"); avatarEl.className = "avatar-circle small"; renderAvatarCircle(avatarEl, user.avatar, user.name);
      const labelEl = document.createElement("span"); labelEl.innerHTML = `<b>${escapeHtml(user.name)}</b> is in the waiting room`;
      textSpan.appendChild(avatarEl); textSpan.appendChild(labelEl);
      const actions = document.createElement("div"); actions.className = "row"; actions.style.flexShrink = "0";
      actions.innerHTML = `<button class="active admit-btn" style="padding:0.4rem 0.7rem;">Admit</button><button class="danger deny-btn" style="padding:0.4rem 0.7rem;">Decline</button>`;
      actions.querySelector(".admit-btn").onclick = () => { delete waitingUsers[user.id]; sendSignal({ type: "join_decision", target: user.id, approved: true }); card.remove(); scheduleParticipantsRender(); };
      actions.querySelector(".decline-btn").onclick = () => { delete waitingUsers[user.id]; sendSignal({ type: "join_decision", target: user.id, approved: false }); card.remove(); scheduleParticipantsRender(); };
      card.appendChild(textSpan); card.appendChild(actions); bannerContainer.appendChild(card);
    }

    function showHostRequestBanner(requesterId, requesterName) {
      document.getElementById(`req-${requesterId}`)?.remove();
      const card = document.createElement("div"); card.id = `req-${requesterId}`; card.className = "banner-card";
      card.innerHTML = `<span class="banner-text">🔔 <b>${escapeHtml(requesterName)}</b> wants to share their screen</span><div class="row" style="flex-shrink:0;"><button class="active allow-btn" style="padding:0.4rem 0.7rem;">Allow</button><button class="danger deny-btn" style="padding:0.4rem 0.7rem;">Deny</button></div>`;
      card.querySelector(".allow-btn").onclick = () => sendScreenDecision(requesterId, true);
      card.querySelector(".deny-btn").onclick = () => sendScreenDecision(requesterId, false);
      bannerContainer.appendChild(card);
    }

    function showApprovedBanner() {
      document.getElementById("approved-banner")?.remove();
      const card = document.createElement("div"); card.id = "approved-banner"; card.className = "banner-card approved";
      card.innerHTML = `<span class="banner-text">✅ Host approved your screen share!</span><div class="row" style="flex-shrink:0;"><button class="active start-approved-btn" style="padding:0.4rem 0.75rem;">Share Now</button><button class="secondary dismiss-btn" style="padding:0.4rem 0.6rem;">✕</button></div>`;
      card.querySelector(".start-approved-btn").onclick = async () => { card.remove(); await launchDisplayMedia(); };
      card.querySelector(".dismiss-btn").onclick = () => card.remove();
      bannerContainer.appendChild(card);
    }

    function attachAudioTrackToRecorder(mediaStreamTrack) {
      if (!isRecordingLocally || !recAudioCtx || !recAudioDest || !mediaStreamTrack || recConnectedTrackIds.has(mediaStreamTrack.id)) return;
      try { recAudioCtx.createMediaStreamSource(new MediaStream([mediaStreamTrack])).connect(recAudioDest); recConnectedTrackIds.add(mediaStreamTrack.id); } catch {}
    }
    function syncAllAudioToRecorder() {
      if (!isRecordingLocally || !lkRoom) return;
      const lm = lkRoom.localParticipant.getTrackPublication(LivekitClient.Track.Source.Microphone);
      if (lm?.track?.mediaStreamTrack) attachAudioTrackToRecorder(lm.track.mediaStreamTrack);
      for (const p of lkRoom.remoteParticipants.values()) {
        for (const pub of p.trackPublications.values()) { if (pub.isSubscribed && pub.track?.kind === LivekitClient.Track.Kind.Audio && pub.track.mediaStreamTrack) attachAudioTrackToRecorder(pub.track.mediaStreamTrack); }
      }
    }
    function drawRecorderFrame(ctx) {
      ctx.fillStyle = "#0f1115"; ctx.fillRect(0, 0, recCanvas.width, recCanvas.height);
      if (!screenStage.classList.contains("hidden") && !screenStageVideo.classList.contains("hidden") && screenStageVideo.videoWidth > 0) { drawContained(ctx, screenStageVideo, 0, 0, recCanvas.width, recCanvas.height); return; }
      if (isSharingScreen && localScreenMirrorVideo && localScreenMirrorVideo.videoWidth > 0) { drawContained(ctx, localScreenMirrorVideo, 0, 0, recCanvas.width, recCanvas.height); return; }
      if (wbOpen && !whiteboardStage.classList.contains("hidden")) { drawContained(ctx, wbCanvas, 0, 0, recCanvas.width, recCanvas.height); return; }
      const tiles = Array.from(videoGrid.querySelectorAll(".video-tile:not(.page-hidden)")), count = Math.max(1, tiles.length), cols = Math.ceil(Math.sqrt(count)), rows = Math.ceil(count / cols), cellW = recCanvas.width / cols, cellH = recCanvas.height / rows;
      tiles.forEach((tile, i) => {
        const x = (i % cols) * cellW, y = Math.floor(i / cols) * cellH;
        ctx.fillStyle = "#181b22"; ctx.fillRect(x + 4, y + 4, cellW - 8, cellH - 8);
        const vid = tile.querySelector("video"), ph = tile.querySelector(".placeholder");
        if (vid && !(ph && !ph.classList.contains("hidden")) && vid.videoWidth > 0) drawContained(ctx, vid, x + 4, y + 4, cellW - 8, cellH - 8);
        else { ctx.fillStyle = "#a1a1aa"; ctx.font = "20px system-ui, sans-serif"; ctx.textAlign = "center"; ctx.fillText(ph?.querySelector(".ph-text")?.textContent || "Camera Off", x + cellW / 2, y + cellH / 2); }
        ctx.fillStyle = "rgba(0,0,0,0.7)"; ctx.fillRect(x + 12, y + cellH - 36, Math.min(cellW - 24, 240), 26);
        ctx.fillStyle = "#fff"; ctx.font = "14px system-ui, sans-serif"; ctx.textAlign = "left";
        ctx.fillText(tile.querySelector(".label")?.textContent || "", x + 18, y + cellH - 18);
      });
    }
    function fixWebmSeekable(rawBlob, durationMs) {
      return new Promise((resolve) => {
        if (typeof ysFixWebmDuration === "function") { try { ysFixWebmDuration(rawBlob, durationMs, (b) => resolve(b || rawBlob), { logger: false }); return; } catch {} }
        resolve(rawBlob);
      });
    }
    async function startHostRecording() {
      if (!isHost || currentHostId !== clientId) { showNoticeBanner("🔒 Only the meeting host can record this meeting.", "denied", 3500); return; }
      if (isRecordingLocally) return;
      const saveRoomId = currentRoomId || "meeting";
      try {
        recCanvas = document.createElement("canvas"); recCanvas.width = 1280; recCanvas.height = 720;
        const ctx = recCanvas.getContext("2d", { alpha: false, desynchronized: true });
        recAudioCtx = new (window.AudioContext || window.webkitAudioContext)();
        if (recAudioCtx.state === "suspended") await recAudioCtx.resume();
        recAudioDest = recAudioCtx.createMediaStreamDestination();
        recSilentOsc = recAudioCtx.createOscillator();
        const sg = recAudioCtx.createGain(); sg.gain.value = 0; recSilentOsc.connect(sg); sg.connect(recAudioDest); recSilentOsc.start();
        isRecordingLocally = true; recConnectedTrackIds = new Set(); syncAllAudioToRecorder(); drawRecorderFrame(ctx);
        try {
          const workerCode = `let t=null; self.onmessage=(e)=>{ if(e.data==="start"){clearInterval(t);t=setInterval(()=>self.postMessage(0),33);} else if(e.data==="stop"){clearInterval(t);t=null;} };`;
          recWorkerUrl = URL.createObjectURL(new Blob([workerCode], { type: "application/javascript" }));
          recWorker = new Worker(recWorkerUrl); recWorker.onmessage = () => drawRecorderFrame(ctx); recWorker.postMessage("start");
        } catch { recDrawTimer = setInterval(() => drawRecorderFrame(ctx), 33); }
        const canvasStream = recCanvas.captureStream(30), combinedStream = new MediaStream([...canvasStream.getVideoTracks(), ...recAudioDest.stream.getAudioTracks()]);
        let mimeType = "";
        for (const c of ["video/webm;codecs=h264,opus", "video/webm;codecs=vp8,opus", "video/webm;codecs=vp9,opus", "video/webm"]) { if (MediaRecorder.isTypeSupported(c)) { mimeType = c; break; } }
        recordedChunks = [];
        try { mediaRecorder = mimeType ? new MediaRecorder(combinedStream, { mimeType, videoBitsPerSecond: 3_500_000 }) : new MediaRecorder(combinedStream); }
        catch { mediaRecorder = new MediaRecorder(combinedStream); }
        mediaRecorder.ondataavailable = (e) => { if (e.data && e.data.size > 0) recordedChunks.push(e.data); };
        recStopPromise = new Promise((resolve) => {
          mediaRecorder.onstop = async () => {
            const totalDurationMs = Math.max(500, Date.now() - recStartTime);
            if (recWorker) { recWorker.postMessage("stop"); recWorker.terminate(); recWorker = null; }
            if (recWorkerUrl) { URL.revokeObjectURL(recWorkerUrl); recWorkerUrl = null; }
            if (recDrawTimer) { clearInterval(recDrawTimer); recDrawTimer = null; }
            canvasStream.getTracks().forEach(t => t.stop());
            if (recSilentOsc) { try { recSilentOsc.stop(); } catch {} recSilentOsc = null; }
            if (recAudioCtx) { recAudioCtx.close().catch(() => {}); recAudioCtx = null; }
            recAudioDest = null; recConnectedTrackIds.clear();
            if (recordedChunks.length === 0) { showNoticeBanner("⚠️ Recording stopped but captured 0 bytes.", "denied", 4500); resolve(); return; }
            const rawBlob = new Blob(recordedChunks, { type: mediaRecorder?.mimeType || mimeType || "video/webm" }); recordedChunks = [];
            const fixedBlob = await fixWebmSeekable(rawBlob, totalDurationMs);
            let serverSavedMsg = "";
            try {
              const hk = localStorage.getItem(`vc_host_key_${saveRoomId}`) || sessionStorage.getItem(`vc_host_key_${saveRoomId}`) || "";
              const resp = await fetch(`/api/recordings/${encodeURIComponent(saveRoomId)}`, { method: "POST", headers: { "Content-Type": "application/octet-stream", "X-Host-Key": hk }, body: fixedBlob });
              if (resp.ok) serverSavedMsg = `Saved to recordings/${(await resp.json()).filename} & Downloads!`;
            } catch {}
            const url = URL.createObjectURL(fixedBlob), a = document.createElement("a");
            a.href = url; a.download = `meeting-${saveRoomId}-${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}.webm`;
            document.body.appendChild(a); a.click(); a.remove(); setTimeout(() => URL.revokeObjectURL(url), 5000);
            showNoticeBanner(`💾 ${serverSavedMsg || "Seekable recording saved to your Downloads folder!"}`, "approved", 6000);
            if (isMainHostServer) loadRecordings();
            resolve();
          };
        });
        recStartTime = Date.now(); mediaRecorder.start(2000); isMeetingRecording = true;
        sendSignal({ type: "recording_started" }); updateRecordingUI(true, getDisplayName()); updateControlButtons();
      } catch (err) { isRecordingLocally = false; showNoticeBanner(`❌ Could not start recording: ${err.message}`, "denied"); }
    }
    async function stopHostRecording(notifyServer = true) {
      if (!isRecordingLocally) return;
      isRecordingLocally = false; isMeetingRecording = false;
      if (notifyServer) sendSignal({ type: "recording_stopped" });
      updateRecordingUI(false); updateControlButtons();
      if (mediaRecorder && mediaRecorder.state !== "inactive") { try { mediaRecorder.requestData(); } catch {} mediaRecorder.stop(); if (recStopPromise) await recStopPromise; }
      mediaRecorder = null; recStopPromise = null;
    }
    function drawContained(ctx, sourceEl, dx, dy, dw, dh) {
      const vw = sourceEl.videoWidth || sourceEl.width, vh = sourceEl.videoHeight || sourceEl.height;
      if (!vw || !vh) return;
      const scale = Math.min(dw / vw, dh / vh), w = vw * scale, h = vh * scale;
      ctx.drawImage(sourceEl, dx + (dw - w) / 2, dy + (dh - h) / 2, w, h);
    }
    function updateRecordingUI(recording, byName = "Host") {
      isMeetingRecording = recording; recordingPill.classList.toggle("hidden", !recording);
      recordingPill.textContent = "● REC"; recordingPill.title = recording ? `Recording by ${byName}` : "";
    }

    function displayCaption(speaker, text) {
      if (!captionsEnabled) return;
      captionBar.classList.remove("hidden"); captionSpeaker.textContent = speaker ? `${speaker}:` : ""; captionText.textContent = text;
      clearTimeout(captionClearTimer);
      captionClearTimer = setTimeout(() => { if (captionsEnabled) { captionSpeaker.textContent = ""; captionText.textContent = whisperReady ? "AI Captions listening..." : (micEnabled ? "Listening for speech..." : "Speaker Only: listening to participants..."); } }, 5000);
    }
    function downsampleTo16k(float32Array, inputSampleRate) {
      if (inputSampleRate === 16000) return float32Array;
      const ratio = inputSampleRate / 16000, newLength = Math.max(1, Math.round(float32Array.length / ratio)), result = new Float32Array(newLength);
      for (let i = 0; i < newLength; i++) { const pos = i * ratio, idx = Math.floor(pos), frac = pos - idx; result[i] = (float32Array[idx] || 0) + ((float32Array[Math.min(idx + 1, float32Array.length - 1)] || float32Array[idx] || 0) - (float32Array[idx] || 0)) * frac; }
      return result;
    }
    function syncWhisperAudioTracks() {
      if (!whisperAudioCtx || !whisperProcessor) return;
      const desired = new Map();
      if (lkRoom && micEnabled) { const lm = lkRoom.localParticipant.getTrackPublication(LivekitClient.Track.Source.Microphone); if (lm?.track?.mediaStreamTrack && !speechRecognition) desired.set(lm.track.mediaStreamTrack.id, lm.track.mediaStreamTrack); }
      if (lkRoom) { for (const p of lkRoom.remoteParticipants.values()) { for (const pub of p.trackPublications.values()) { if (pub.isSubscribed && pub.track?.kind === LivekitClient.Track.Kind.Audio && pub.track.mediaStreamTrack) desired.set(pub.track.mediaStreamTrack.id, pub.track.mediaStreamTrack); } } }
      for (const [tid, node] of whisperSources.entries()) { if (!desired.has(tid)) { try { node.disconnect(); } catch {} whisperSources.delete(tid); } }
      for (const [tid, track] of desired.entries()) { if (!whisperSources.has(tid)) { try { const src = whisperAudioCtx.createMediaStreamSource(new MediaStream([track])); src.connect(whisperProcessor); whisperSources.set(tid, src); } catch {} } }
    }
    function getActiveSpeakerLabelForWhisper() {
      if (micEnabled && localMicSpeaking && !speechRecognition) return `${getDisplayName()} (You)`;
      for (const id of lkSpeakingIds) { if (id !== "local" && peerNames[id]) return peerNames[id]; }
      if (lastActiveSpeakerTileId && lastActiveSpeakerTileId !== "local" && peerNames[lastActiveSpeakerTileId]) return peerNames[lastActiveSpeakerTileId];
      return Object.values(peerNames)[0] || (micEnabled ? `${getDisplayName()} (You)` : "Participant");
    }
    function cleanWhisperTranscript(text) {
      const t = (text || "").trim();
      if (!t || /^(\[.*\]|\(.*\))$/.test(t) || /^(you|thank you\.?|thanks for watching!?|\.+|,+)$/i.test(t)) return "";
      return t;
    }
    function startWhisperCaptions() {
      if (!captionsEnabled || whisperAudioCtx) return;
      displayCaption("", "⏳ Loading AI Captions (first time ~40MB, cached after)...");
      try {
        const AudioCtx = window.AudioContext || window.webkitAudioContext; whisperAudioCtx = new AudioCtx({ sampleRate: 16000 });
        if (whisperAudioCtx.state === "suspended") whisperAudioCtx.resume().catch(() => {});
        whisperProcessor = whisperAudioCtx.createScriptProcessor(4096, 1, 1);
        whisperSilentGain = whisperAudioCtx.createGain(); whisperSilentGain.gain.value = 0;
        whisperProcessor.connect(whisperSilentGain); whisperSilentGain.connect(whisperAudioCtx.destination);
        if (!whisperWorker) {
          const workerScript = `
            import { pipeline, env } from "https://cdn.jsdelivr.net/npm/@huggingface/transformers@3.5.1";
            env.allowLocalModels = false; let transcriber = null;
            async function init() {
              try {
                try { transcriber = await pipeline("automatic-speech-recognition", "Xenova/whisper-tiny.en", { dtype: "q8" }); }
                catch { transcriber = await pipeline("automatic-speech-recognition", "Xenova/whisper-tiny.en"); }
                self.postMessage({ type: "ready" });
              } catch (err) { self.postMessage({ type: "error", message: err?.message || String(err) }); }
            }
            self.onmessage = async (e) => {
              if (e.data.type === "init") await init();
              else if (e.data.type === "transcribe" && transcriber) {
                try { const res = await transcriber(e.data.audio, { language: "english", task: "transcribe" }); self.postMessage({ type: "result", text: res?.text || "", speaker: e.data.speaker }); }
                catch { self.postMessage({ type: "done" }); }
              }
            };
          `;
          whisperWorkerUrl = URL.createObjectURL(new Blob([workerScript], { type: "application/javascript" }));
          whisperWorker = new Worker(whisperWorkerUrl, { type: "module" });
          whisperWorker.onmessage = (e) => {
            const msg = e.data;
            if (msg.type === "ready") { whisperReady = true; if (captionsEnabled) displayCaption("", "✅ AI Captions Ready (listening to speech...)"); }
            else if (msg.type === "error") { whisperReady = false; whisperBusy = false; if (captionsEnabled) displayCaption("", `⚠️ AI Captions unavailable (${msg.message}).`); }
            else if (msg.type === "result") {
              whisperBusy = false; const clean = cleanWhisperTranscript(msg.text);
              if (clean && captionsEnabled) {
                displayCaption(msg.speaker || "Speaker", clean);
                if (micEnabled && !speechRecognition && msg.speaker?.includes("(You)")) sendRoomData({ type: "caption", text: clean, is_final: true });
              }
            } else if (msg.type === "done") whisperBusy = false;
          };
          whisperWorker.postMessage({ type: "init" });
        }
        whisperBuffers = []; whisperSampleCount = 0; whisperVoiceFrames = 0;
        whisperProcessor.onaudioprocess = (ev) => {
          if (!captionsEnabled || !whisperReady) return;
          const input = ev.inputBuffer.getChannelData(0); let sumSq = 0; for (let i = 0; i < input.length; i++) sumSq += input[i] * input[i];
          if (Math.sqrt(sumSq / input.length) > 0.012) { whisperVoiceFrames++; whisperActiveSpeakerLabel = getActiveSpeakerLabelForWhisper(); }
          if (whisperVoiceFrames > 0) {
            const down = downsampleTo16k(new Float32Array(input), whisperAudioCtx.sampleRate);
            whisperBuffers.push(down); whisperSampleCount += down.length;
            if (whisperSampleCount >= 16000 * 2.5) {
              if (!whisperBusy && whisperVoiceFrames >= 2) {
                const merged = new Float32Array(whisperSampleCount); let off = 0; for (const b of whisperBuffers) { merged.set(b, off); off += b.length; }
                whisperBusy = true; whisperWorker.postMessage({ type: "transcribe", audio: merged, speaker: whisperActiveSpeakerLabel }, [merged.buffer]);
              }
              whisperBuffers = []; whisperSampleCount = 0; whisperVoiceFrames = 0;
            }
          }
        };
        syncWhisperAudioTracks();
      } catch { displayCaption("", "⚠️ Could not initialize audio captions in this browser."); }
    }
    function stopWhisperCaptions() {
      for (const s of whisperSources.values()) { try { s.disconnect(); } catch {} }
      whisperSources.clear();
      if (whisperProcessor) { whisperProcessor.onaudioprocess = null; try { whisperProcessor.disconnect(); } catch {} whisperProcessor = null; }
      if (whisperSilentGain) { try { whisperSilentGain.disconnect(); } catch {} whisperSilentGain = null; }
      if (whisperAudioCtx) { whisperAudioCtx.close().catch(() => {}); whisperAudioCtx = null; }
      whisperBuffers = []; whisperSampleCount = 0; whisperVoiceFrames = 0; whisperBusy = false;
    }
    function startSpeechRecognition() {
      if (!captionsEnabled) return;
      if (!micEnabled) { startWhisperCaptions(); return; }
      const SpeechRec = window.SpeechRecognition || window.webkitSpeechRecognition;
      if (!SpeechRec) { startWhisperCaptions(); return; }
      if (speechRecognition) return;
      const rec = new SpeechRec(); rec.continuous = true; rec.interimResults = true; rec.lang = navigator.language || "en-US";
      rec.onresult = (event) => {
        const res = event.results[event.results.length - 1], transcript = res[0].transcript.trim();
        if (!transcript) return;
        displayCaption(`${getDisplayName()} (You)`, transcript);
        const now = Date.now();
        if (res.isFinal || now - lastCaptionSentAt > 350) { lastCaptionSentAt = now; sendRoomData({ type: "caption", text: transcript, is_final: res.isFinal }); }
      };
      rec.onerror = (e) => { if (e.error === "network" || e.error === "service-not-allowed") { stopSpeechRecognition(); startWhisperCaptions(); } else if (e.error === "not-allowed") stopSpeechRecognition(); };
      rec.onend = () => { speechRecognition = null; if (captionsEnabled && micEnabled && hasJoinedRoom && !whisperAudioCtx) setTimeout(() => startSpeechRecognition(), 300); };
      try { rec.start(); speechRecognition = rec; startWhisperCaptions(); } catch { startWhisperCaptions(); }
    }
    function stopSpeechRecognition() { if (speechRecognition) { const r = speechRecognition; speechRecognition = null; try { r.onend = null; r.stop(); } catch {} } }
    function toggleAutoCaptions() {
      captionsEnabled = !captionsEnabled;
      if (captionsEnabled) { captionBar.classList.remove("hidden"); captionSpeaker.textContent = ""; captionText.textContent = "Starting Live Captions..."; startSpeechRecognition(); }
      else { stopSpeechRecognition(); stopWhisperCaptions(); clearTimeout(captionClearTimer); captionBar.classList.add("hidden"); }
      updateControlButtons();
    }
    ccBtn.onclick = () => { toggleAutoCaptions(); controlsDock.classList.remove("more-open"); };

    async function loadRecordings() {
      if (!isMainHostServer) return;
      try {
        const res = await fetch("/api/recordings"); if (!res.ok) return;
        const recs = await res.json(); recordingList.innerHTML = "";
        if (recs.length === 0) { recordingList.innerHTML = `<div class="meeting-sub">No recordings saved yet.</div>`; return; }
        for (const r of recs) {
          const item = document.createElement("div"); item.className = "meeting-item";
          item.innerHTML = `<div class="meeting-meta"><span class="meeting-title">🎬 ${escapeHtml(r.filename)}</span><span class="meeting-sub">${r.size_mb} MB</span></div><div class="row"><button class="secondary watch-rec-btn">Watch</button><a class="btn-link" href="${escapeHtml(r.url)}?download=1" download="${escapeHtml(r.filename)}">Download</a></div>`;
          item.querySelector(".watch-rec-btn").onclick = () => openRecordingPlayer(r.filename, r.url);
          recordingList.appendChild(item);
        }
      } catch { recordingList.innerHTML = `<div class="meeting-sub">Could not load recordings.</div>`; }
    }
    async function loadMeetings() {
      if (!isMainHostServer) return;
      try {
        const res = await fetch("/api/meetings"); if (!res.ok) return;
        const meetings = await res.json(); meetingList.innerHTML = "";
        if (meetings.length === 0) { meetingList.innerHTML = `<div class="meeting-sub">No meetings scheduled yet.</div>`; return; }
        for (const m of meetings) {
          const item = document.createElement("div"); item.className = "meeting-item";
          const link = getMeetingUrl(m.id);
          item.innerHTML = `<div class="meeting-meta"><span class="meeting-title">${escapeHtml(m.title)}</span><span class="meeting-sub">Code: <b>${escapeHtml(m.id)}</b> • ${escapeHtml(formatDateTime(m.scheduled_at))} • ⏱️ ${escapeHtml(formatDurationLabel(m.duration_minutes))} • ${m.require_join_approval ? "Waiting Room" : "Direct Join"} • ${m.require_screen_approval ? "Screen Approval" : "Open Share"}${m.record_meeting ? " • Auto-Record" : ""}</span></div><div class="row"><button class="secondary copy-m-btn">Copy Link</button><button class="join-m-btn">Join</button><button class="danger del-m-btn">Delete</button></div>`;
          item.querySelector(".copy-m-btn").onclick = (e) => copyText(link, e.target);
          item.querySelector(".join-m-btn").onclick = () => enterMeetingScreen(m.id, m.title);
          item.querySelector(".del-m-btn").onclick = async () => { await fetch(`/api/meetings/${encodeURIComponent(m.id)}`, { method: "DELETE" }); loadMeetings(); };
          meetingList.appendChild(item);
        }
      } catch { meetingList.innerHTML = `<div class="meeting-sub">Failed to load meetings.</div>`; }
    }

    instantBtn.onclick = async () => {
      const res = await fetch("/api/meetings", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ title: `${getDisplayName()}'s Meeting`, host_name: getDisplayName(), scheduled_at: null, duration_minutes: Number(instantDuration.value || 0), require_join_approval: instantJoinMode.value === "approval", require_screen_approval: instantApprovalChk.checked, record_meeting: instantRecordChk.checked }) });
      const meeting = await res.json();
      if (meeting.host_key) localStorage.setItem(`vc_host_key_${meeting.id}`, meeting.host_key);
      history.pushState({}, "", `/room/${meeting.id}`);
      enterMeetingScreen(meeting.id, meeting.title);
    };
    schedBtn.onclick = async () => {
      const title = schedTitle.value.trim() || "Scheduled Meeting", dtVal = schedTime.value;
      if (!dtVal) return alert("Please pick a date and time to schedule.");
      const res = await fetch("/api/meetings", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ title, host_name: getDisplayName(), scheduled_at: new Date(dtVal).toISOString(), duration_minutes: Number(schedDuration.value || 0), require_join_approval: schedJoinMode.value === "approval", require_screen_approval: schedApprovalChk.checked, record_meeting: schedRecordChk.checked }) });
      const meeting = await res.json();
      if (meeting.host_key) localStorage.setItem(`vc_host_key_${meeting.id}`, meeting.host_key);
      schedLinkBox.textContent = `Scheduled (${formatDurationLabel(meeting.duration_minutes)})! Link: ${getMeetingUrl(meeting.id)}`; schedResult.classList.remove("hidden"); schedTitle.value = "";
      if (isMainHostServer) loadMeetings();
    };
    refreshListBtn.onclick = loadMeetings; refreshRecBtn.onclick = loadRecordings;

    async function checkUrlForRoom() {
      const match = location.pathname.match(/^\/room\/([^/]+)$/);
      if (!match) { directRoomCard.classList.add("hidden"); lobbyHomeSections.classList.remove("hidden"); return; }
      lobbyHomeSections.classList.add("hidden");
      const roomId = decodeURIComponent(match[1]), meeting = await (await fetch(`/api/meetings/${encodeURIComponent(roomId)}`)).json();
      directRoomCard.classList.remove("hidden"); directRoomTitle.textContent = meeting.title || `Room ${roomId}`;
      const jt = meeting.require_join_approval ? "Waiting Room Enabled" : "Direct Join";
      const dt = formatDurationLabel(meeting.duration_minutes);
      directRoomTime.textContent = meeting.scheduled_at ? `Scheduled for: ${formatDateTime(meeting.scheduled_at)} • Duration: ${dt} • ${jt}` : `Meeting Code: ${roomId} • Duration: ${dt} • ${jt}`;
      directRoomLink.textContent = getMeetingUrl(roomId);
      directJoinBtn.onclick = () => enterMeetingScreen(roomId, meeting.title);
      directCopyBtn.onclick = (e) => copyText(getMeetingUrl(roomId), e.target);
    }
    joinBtn.onclick = async () => {
      const val = roomInput.value.trim(); if (!val) return alert("Enter a meeting code or room name.");
      const match = val.match(/\/room\/([^/?#]+)/), roomId = decodeURIComponent(match ? match[1] : val);
      let title = `Room ${roomId}`;
      try { const m = await (await fetch(`/api/meetings/${encodeURIComponent(roomId)}`)).json(); if (m?.title) title = m.title; } catch {}
      history.pushState({}, "", `/room/${encodeURIComponent(roomId)}`); enterMeetingScreen(roomId, title);
    };

    function refreshDmDropdown() {
      const prevSelected = dmTarget.value; dmTarget.innerHTML = "";
      const entries = Object.entries(peerNames);
      if (entries.length === 0) { const opt = document.createElement("option"); opt.value = ""; opt.textContent = "No other participants in room"; dmTarget.appendChild(opt); }
      else {
        for (const [id, name] of entries) { const opt = document.createElement("option"); opt.value = id; opt.textContent = `${name} (${id.slice(0, 4)})`; dmTarget.appendChild(opt); }
        if (prevSelected && peerNames[prevSelected]) dmTarget.value = prevSelected;
      }
      scheduleParticipantsRender();
    }
    function updateUnreadBadges() {
      badgePublic.textContent = unreadPublic; badgePublic.classList.toggle("hidden", unreadPublic === 0);
      badgePrivate.textContent = unreadPrivate; badgePrivate.classList.toggle("hidden", unreadPrivate === 0);
      const total = unreadPublic + unreadPrivate; chatTotalBadge.textContent = total; chatTotalBadge.classList.toggle("hidden", total === 0 || !chatPanel.classList.contains("hidden"));
    }
    function switchChatTab(tab) {
      activeChatTab = tab; const isPub = tab === "public";
      tabPublic.classList.toggle("active-tab", isPub); tabPrivate.classList.toggle("active-tab", !isPub);
      publicMessages.classList.toggle("hidden", !isPub); privateMessages.classList.toggle("hidden", isPub);
      dmBar.classList.toggle("hidden", isPub); chatInput.placeholder = isPub ? "Message everyone..." : "Send a private message...";
      if (isPub) unreadPublic = 0; else unreadPrivate = 0; updateUnreadBadges();
    }
    tabPublic.onclick = () => switchChatTab("public"); tabPrivate.onclick = () => switchChatTab("private");
    toggleChatBtn.onclick = () => {
      const opening = chatPanel.classList.contains("hidden"); chatPanel.classList.toggle("hidden", !opening);
      if (opening) { participantsPanel.classList.add("hidden"); controlsDock.classList.remove("more-open"); syncPeersFromLiveKit(); if (activeChatTab === "public") unreadPublic = 0; else unreadPrivate = 0; }
      updateUnreadBadges();
    };
    function appendChatMessage({ container, senderName, text, isSelf, isDm = false, dmLabel = "", replyToId = null }) {
      const item = document.createElement("div"); item.className = `msg-item${isSelf ? " self" : ""}${isDm ? " dm" : ""}`;
      item.innerHTML = `<div class="msg-meta">${isDm ? `<span class="dm-tag">${escapeHtml(dmLabel)}</span>` : ""}${escapeHtml(senderName)} • ${new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}</div><div class="msg-bubble">${escapeHtml(text)}</div>`;
      if (isDm && !isSelf && replyToId && peerNames[replyToId]) { item.title = "Click to reply privately"; item.onclick = () => { dmTarget.value = replyToId; chatInput.focus(); }; }
      container.appendChild(item); container.scrollTop = container.scrollHeight;
    }

    function handleIncomingRoomEvent(msg) {
      if (!msg || !msg.type || (msg.msgId && rememberMsgId(msg.msgId))) return;
      const senderId = msg.sender; if (senderId === clientId) return;
      const senderName = msg.sender_name || peerNames[senderId] || "Participant";
      if (msg.type === "screen_share_request") {
        if (isHost && currentHostId === clientId && senderId) { pendingScreenRequests[senderId] = { id: senderId, name: senderName }; playWaitingKnockSound(); showHostRequestBanner(senderId, senderName); scheduleParticipantsRender(); }
      } else if (msg.type === "screen_share_approved") {
        if (!msg.target || msg.target === clientId) { screenRequestPending = false; screenShareApproved = true; updateControlButtons(); showApprovedBanner(); }
      } else if (msg.type === "screen_share_denied") {
        if (!msg.target || msg.target === clientId) { screenRequestPending = false; screenShareApproved = false; updateControlButtons(); showNoticeBanner(msg.reason || "Screen share request was denied.", "denied", 4500); }
      } else if (msg.type === "caption") { displayCaption(senderName, msg.text); }
      else if (msg.type === "reaction") { showReaction(msg.emoji, senderName, senderId); }
      else if (msg.type === "chat_public") {
        appendChatMessage({ container: publicMessages, senderName, text: msg.text, isSelf: false });
        if (chatPanel.classList.contains("hidden") || activeChatTab !== "public") { unreadPublic++; updateUnreadBadges(); }
      } else if (msg.type === "chat_private") {
        if (msg.target && msg.target !== clientId) return;
        appendChatMessage({ container: privateMessages, senderName, text: msg.text, isSelf: false, isDm: true, dmLabel: "Private", replyToId: senderId });
        if (chatPanel.classList.contains("hidden") || activeChatTab !== "private") { unreadPrivate++; updateUnreadBadges(); }
      }
    }

    function sendRoomData(payload, targetId = null) {
      const fullPayload = { ...payload, msgId: `${clientId}-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`, sender: clientId, sender_name: getDisplayName() };
      if (targetId) fullPayload.target = targetId;
      sendSignal(fullPayload);
      if (lkRoom && lkRoom.state === "connected") {
        try { const bytes = new TextEncoder().encode(JSON.stringify(fullPayload)), opts = { reliable: true }; if (targetId) opts.destinationIdentities = [targetId]; lkRoom.localParticipant.publishData(bytes, opts).catch(() => {}); } catch {}
      }
    }
    function sendChatMessage() {
      const text = chatInput.value.trim(); if (!text) return;
      if (activeChatTab === "public") { sendRoomData({ type: "chat_public", text }); appendChatMessage({ container: publicMessages, senderName: `${getDisplayName()} (You)`, text, isSelf: true }); }
      else {
        const targetId = dmTarget.value; if (!targetId || !peerNames[targetId]) return alert("Select a participant to send a personal message.");
        sendRoomData({ type: "chat_private", text }, targetId); appendChatMessage({ container: privateMessages, senderName: "You", text, isSelf: true, isDm: true, dmLabel: `To ${peerNames[targetId]}` });
      }
      chatInput.value = "";
    }
    sendChatBtn.onclick = sendChatMessage; chatInput.addEventListener("keydown", (e) => { if (e.key === "Enter") sendChatMessage(); });

    function showReaction(emoji, senderName, tileId) {
      const el = document.createElement("div"); el.className = "floating-reaction"; el.style.left = `${15 + Math.random() * 70}%`;
      el.innerHTML = `<span class="emoji">${escapeHtml(emoji)}</span><span class="who">${escapeHtml(senderName)}</span>`;
      el.addEventListener("animationend", () => el.remove()); reactionLayer.appendChild(el);
      const tile = document.getElementById(`tile-${tileId}`);
      if (tile) { let b = tile.querySelector(".tile-reaction"); if (!b) { b = document.createElement("div"); b.className = "tile-reaction"; tile.appendChild(b); } b.textContent = emoji; clearTimeout(tileReactionTimers[tileId]); tileReactionTimers[tileId] = setTimeout(() => b.remove(), 3000); }
    }
    document.querySelectorAll(".react-btn").forEach(btn => { btn.onclick = () => { sendRoomData({ type: "reaction", emoji: btn.dataset.emoji }); showReaction(btn.dataset.emoji, getDisplayName(), "local"); }; });

    function ensureVideoTile(id, labelText, isMuted = true, avatarUrl = "") {
      let tile = document.getElementById(`tile-${id}`);
      const cleanName = labelText.replace(/\s*\(You\)$/, "");
      if (!tile) {
        tile = document.createElement("div"); tile.id = `tile-${id}`; tile.className = "video-tile";
        const video = document.createElement("video"); video.autoplay = true; video.playsInline = true; video.muted = true;
        const placeholder = document.createElement("div"); placeholder.className = "placeholder";
        const avatarEl = document.createElement("div"); avatarEl.className = "avatar-circle large"; renderAvatarCircle(avatarEl, avatarUrl, cleanName);
        const phText = document.createElement("div"); phText.className = "ph-text"; phText.textContent = "Camera Off";
        placeholder.appendChild(avatarEl); placeholder.appendChild(phText);
        const actions = document.createElement("div"); actions.className = "tile-actions";
        const pinBtn = document.createElement("button"); pinBtn.type = "button"; pinBtn.className = "pin-btn"; pinBtn.textContent = "📌 Pin"; pinBtn.title = "Pin / Spotlight";
        pinBtn.onclick = (e) => { e.stopPropagation(); if (pinnedTileId === id) pinnedTileId = null; else { pinnedTileId = id; if (currentViewMode === "grid" || currentViewMode === "compact") { applyViewMode("speaker"); return; } } updateFeaturedTile(); };
        const snapBtn = document.createElement("button"); snapBtn.type = "button"; snapBtn.className = "snap-btn"; snapBtn.textContent = "📸"; snapBtn.title = `Screenshot ${cleanName}'s camera`;
        snapBtn.onclick = (e) => { e.stopPropagation(); takeTileScreenshot(id, cleanName); };
        actions.appendChild(pinBtn); actions.appendChild(snapBtn);
        const label = document.createElement("div"); label.className = "label"; label.textContent = labelText;
        tile.appendChild(video); tile.appendChild(placeholder); tile.appendChild(actions); tile.appendChild(label);
        videoGrid.appendChild(tile);
        if (!tileOrder.includes(id)) tileOrder.push(id);
        videoGrid.classList.toggle("single-tile", tileOrder.length <= 1);
        updateFeaturedTile(); renderPagination();
      } else {
        tile.querySelector(".label").textContent = labelText;
        const avatarEl = tile.querySelector(".placeholder .avatar-circle");
        if (avatarEl) renderAvatarCircle(avatarEl, avatarUrl, cleanName);
      }
      return tile;
    }

    function setTilePlaceholder(id, showVideo, placeholderText = "Camera Off") {
      const tile = document.getElementById(`tile-${id}`); if (!tile) return;
      const ph = tile.querySelector(".placeholder"), phText = ph.querySelector(".ph-text");
      if (phText) phText.textContent = placeholderText; ph.classList.toggle("hidden", showVideo);
    }
    function removeVideoTile(id) {
      if (pinnedTileId === id) pinnedTileId = null;
      if (lastActiveSpeakerTileId === id) lastActiveSpeakerTileId = null;
      document.getElementById(`tile-${id}`)?.remove();
      tileOrder = tileOrder.filter(x => x !== id);
      videoGrid.classList.toggle("single-tile", tileOrder.length <= 1);
      updateFeaturedTile(); renderPagination();
    }
    function renderPagination() {
      const pageSize = getPageSize(), totalPages = Math.max(1, Math.ceil(tileOrder.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;
      const start = (currentPage - 1) * pageSize, end = start + pageSize;
      tileOrder.forEach((id, idx) => { const tile = document.getElementById(`tile-${id}`); if (tile) tile.classList.toggle("page-hidden", !tile.classList.contains("featured-tile") && (idx < start || idx >= end)); });
      paginationBar.classList.toggle("hidden", totalPages <= 1);
      pageIndicator.textContent = `Page ${currentPage} / ${totalPages}`;
      prevPageBtn.disabled = currentPage <= 1; nextPageBtn.disabled = currentPage >= totalPages;
      applySubscriptionEfficiency();
    }
    prevPageBtn.onclick = () => { if (currentPage > 1) { currentPage--; renderPagination(); } };
    nextPageBtn.onclick = () => { if (currentPage < Math.max(1, Math.ceil(tileOrder.length / getPageSize()))) { currentPage++; renderPagination(); } };

    async function checkDeviceExists(kind) { if (!navigator.mediaDevices?.getUserMedia) return false; if (navigator.mediaDevices.enumerateDevices) return (await navigator.mediaDevices.enumerateDevices()).some(d => d.kind === kind); return true; }
    function handleMediaError(err, typeLabel) {
      if (err.name === "NotFoundError" || err.name === "DevicesNotFoundError" || err.name === "OverconstrainedError") showNoticeBanner(`❌ No ${typeLabel} found on this device.`, "denied", 5000);
      else if (err.name === "NotAllowedError" || err.name === "PermissionDeniedError") showNoticeBanner(`⚠️ ${typeLabel} permission denied in browser settings.`, "denied", 5000);
      else if (err.name === "NotReadableError" || err.name === "TrackStartError") showNoticeBanner(`⚠️ ${typeLabel} is already in use by another app.`, "denied", 5000);
      else showNoticeBanner(`❌ Could not enable ${typeLabel}: ${err.message}`, "denied", 5000);
    }

    async function enableMicrophone() {
      if (!(await checkDeviceExists("audioinput"))) { showNoticeBanner("❌ No microphone found on this device. Using Computer Speaker Only.", "denied"); return false; }
      if (!lkRoom) {
        try { const s = await navigator.mediaDevices.getUserMedia({ audio: MIC_CONSTRAINTS }); s.getTracks().forEach(t => t.stop()); wantMicOnJoin = true; micEnabled = true; updateControlButtons(); return true; }
        catch (err) { handleMediaError(err, "microphone"); return false; }
      }
      try {
        await lkRoom.localParticipant.setMicrophoneEnabled(true, MIC_CONSTRAINTS); micEnabled = true;
        const lp = lkRoom.localParticipant.getTrackPublication(LivekitClient.Track.Source.Microphone);
        if (lp?.track?.mediaStreamTrack) startLocalVoiceMonitor(lp.track.mediaStreamTrack);
        syncAllAudioToRecorder(); if (captionsEnabled) { startSpeechRecognition(); syncWhisperAudioTracks(); }
        updateControlButtons(); return true;
      } catch (err) { handleMediaError(err, "microphone"); micEnabled = false; updateControlButtons(); return false; }
    }
    async function useSpeakerOnly() {
      wantMicOnJoin = false; micEnabled = false; stopLocalVoiceMonitor(); stopSpeechRecognition();
      if (lkRoom) await lkRoom.localParticipant.setMicrophoneEnabled(false);
      if (captionsEnabled) startWhisperCaptions(); syncWhisperAudioTracks(); updateControlButtons();
    }

    async function enableCamera() {
      if (!(await checkDeviceExists("videoinput"))) { showNoticeBanner("❌ No camera found on this device.", "denied", 5000); return false; }
      if (!lkRoom) {
        try { const s = await navigator.mediaDevices.getUserMedia({ video: { width: { ideal: 1920 }, height: { ideal: 1080 }, frameRate: { ideal: 30 } } }); ensureVideoTile("local", `${getDisplayName()} (You)`, true, localAvatar).querySelector("video").srcObject = s; setTilePlaceholder("local", true, "Camera Disabled"); wantCamOnJoin = true; camEnabled = true; updateControlButtons(); return true; }
        catch (err) { handleMediaError(err, "camera"); return false; }
      }
      try {
        await lkRoom.localParticipant.setCameraEnabled(true, { resolution: LivekitClient.VideoPresets.h1080.resolution }); camEnabled = true;
        const pub = lkRoom.localParticipant.getTrackPublication(LivekitClient.Track.Source.Camera), lv = ensureVideoTile("local", `${getDisplayName()} (You)`, true, localAvatar).querySelector("video");
        if (pub?.track) pub.track.attach(lv);
        setTilePlaceholder("local", true, "Camera Disabled"); updateControlButtons(); return true;
      } catch (err) { handleMediaError(err, "camera"); camEnabled = false; updateControlButtons(); return false; }
    }
    async function disableCamera() {
      wantCamOnJoin = false; camEnabled = false;
      const lv = document.getElementById("tile-local")?.querySelector("video");
      if (lkRoom) { const pub = lkRoom.localParticipant.getTrackPublication(LivekitClient.Track.Source.Camera); if (pub?.track && lv) pub.track.detach(lv); await lkRoom.localParticipant.setCameraEnabled(false); }
      else if (lv?.srcObject) { lv.srcObject.getTracks().forEach(t => t.stop()); lv.srcObject = null; }
      setTilePlaceholder("local", false, "Camera Disabled"); updateControlButtons();
    }

    function showRemoteScreenStage(presenterName, track, publication) {
      activeScreenPublication = publication || null;
      screenStage.classList.remove("hidden"); screenStageVideo.classList.remove("hidden"); screenFullscreenBtn.classList.remove("hidden");
      onFullscreenChange(); screenStageLabel.textContent = `🖥️ ${presenterName} is presenting (1080p HD)`;
      track.attach(screenStageVideo); applyHighQualityScreenShare();
    }
    async function hideScreenStage() {
      activeScreenPublication = null; await exitScreenFullscreenIfActive();
      screenStage.classList.add("hidden"); screenFullscreenBtn.classList.add("hidden"); screenStageVideo.classList.remove("hidden"); screenStageVideo.srcObject = null;
      if (localScreenMirrorVideo) { localScreenMirrorVideo.srcObject = null; localScreenMirrorVideo = null; }
    }
    async function launchDisplayMedia() {
      if (!lkRoom) return;
      const isSafari = /^((?!chrome|android).)*safari/i.test(navigator.userAgent);
      try { await lkRoom.localParticipant.setScreenShareEnabled(true, { audio: false, selfBrowserSurface: "exclude", contentHint: "detail", ...(isSafari ? {} : { resolution: LivekitClient.ScreenSharePresets.h1080fps30.resolution }) }, { screenShareEncoding: { maxBitrate: 6_000_000, maxFramerate: 30 }, simulcast: false, degradationPreference: "maintain-resolution" }); }
      catch { return; }
      isSharingScreen = true; screenShareApproved = false; screenRequestPending = false; sendSignal({ type: "screen_share_started" });
      const pub = lkRoom.localParticipant.getTrackPublication(LivekitClient.Track.Source.ScreenShare);
      if (pub?.track?.mediaStreamTrack) { localScreenMirrorVideo = document.createElement("video"); localScreenMirrorVideo.muted = true; localScreenMirrorVideo.autoplay = true; localScreenMirrorVideo.playsInline = true; localScreenMirrorVideo.srcObject = new MediaStream([pub.track.mediaStreamTrack]); localScreenMirrorVideo.play().catch(() => {}); }
      screenStage.classList.remove("hidden"); screenStageVideo.classList.add("hidden"); screenFullscreenBtn.classList.add("hidden");
      screenStageLabel.textContent = "🖥️ You are sharing your screen in 1080p HD"; updateControlButtons();
    }
    async function stopScreenShare(notifyServer = true) {
      isSharingScreen = false;
      if (lkRoom) try { await lkRoom.localParticipant.setScreenShareEnabled(false); } catch {}
      if (notifyServer) sendSignal({ type: "screen_share_stopped" });
      await hideScreenStage(); updateControlButtons();
    }

    function applySpeakerMuteState() {
      audioContainer.querySelectorAll("audio").forEach((el) => { el.muted = speakerMuted; });
    }
    function attachRemoteAudio(track) {
      if (!track || track.kind !== LivekitClient.Track.Kind.Audio) return;
      const elId = `audio-${track.sid}`; if (document.getElementById(elId)) return;
      const audioEl = track.attach(); audioEl.id = elId; audioEl.autoplay = true; audioEl.playsInline = true; audioEl.muted = speakerMuted; audioContainer.appendChild(audioEl);
      if (track.mediaStreamTrack) attachAudioTrackToRecorder(track.mediaStreamTrack);
      syncWhisperAudioTracks();
    }

    async function connectLiveKit(url, token) {
      if (typeof LivekitClient === "undefined") { showNoticeBanner("❌ LiveKit client script failed to load.", "denied", 0); return; }
      if (lkRoom) { try { await lkRoom.disconnect(); } catch {} lkRoom = null; }

      lkRoom = new LivekitClient.Room({
        adaptiveStream: { pixelDensity: "screen" },
        dynacast: true,
        audioCaptureDefaults: MIC_CONSTRAINTS,
        videoCaptureDefaults: { resolution: LivekitClient.VideoPresets.h1080.resolution },
        publishDefaults: {
          audioPreset: LivekitClient.AudioPresets.speech, dtx: true, red: true, simulcast: true,
          videoEncoding: LivekitClient.VideoPresets.h1080.encoding,
          videoSimulcastLayers: [LivekitClient.VideoPresets.h720, LivekitClient.VideoPresets.h360, LivekitClient.VideoPresets.h180],
          screenShareEncoding: { maxBitrate: 6_000_000, maxFramerate: 30 }, degradationPreference: "maintain-resolution"
        }
      });

      lkRoom.on(LivekitClient.RoomEvent.ParticipantConnected, (p) => {
        if (!p.identity || p.identity === clientId) return;
        peerNames[p.identity] = p.name || peerNames[p.identity] || `User ${p.identity}`;
        ensureVideoTile(p.identity, peerNames[p.identity], true, peerAvatars[p.identity] || "");
        scheduleDmRefresh();
      });
      lkRoom.on(LivekitClient.RoomEvent.ParticipantDisconnected, (p) => {
        const pid = p.identity; delete peerNames[pid]; delete peerAvatars[pid]; delete pendingScreenRequests[pid]; lkSpeakingIds.delete(pid);
        document.getElementById(`req-${pid}`)?.remove(); removeVideoTile(pid); scheduleDmRefresh(); syncWhisperAudioTracks();
      });
      lkRoom.on(LivekitClient.RoomEvent.DataReceived, (payload, participant) => {
        try {
          const msg = JSON.parse(new TextDecoder().decode(payload));
          if (participant && !msg.sender) msg.sender = participant.identity;
          if (participant && !msg.sender_name) msg.sender_name = participant.name || peerNames[participant.identity];
          handleIncomingRoomEvent(msg);
        } catch {}
      });
      lkRoom.on(LivekitClient.RoomEvent.ActiveSpeakersChanged, (speakers) => {
        lkSpeakingIds.clear(); for (const sp of speakers) lkSpeakingIds.add(sp.identity === clientId ? "local" : sp.identity); updateSpeakingUI();
      });
      lkRoom.on(LivekitClient.RoomEvent.TrackSubscribed, (track, publication, participant) => {
        const pid = participant.identity, pName = participant.name || peerNames[pid] || `User ${pid}`, pAvatar = peerAvatars[pid] || "";
        if (!peerNames[pid]) { peerNames[pid] = pName; scheduleDmRefresh(); }
        if (track.kind === LivekitClient.Track.Kind.Audio) attachRemoteAudio(track);
        else if (publication.source === LivekitClient.Track.Source.ScreenShare) showRemoteScreenStage(pName, track, publication);
        else if (publication.source === LivekitClient.Track.Source.Camera) {
          const tile = ensureVideoTile(pid, pName, true, pAvatar); track.attach(tile.querySelector("video"));
          setTilePlaceholder(pid, !publication.isMuted, "Camera Off"); applySubscriptionEfficiency();
        }
        scheduleParticipantsRender();
      });
      lkRoom.on(LivekitClient.RoomEvent.TrackUnsubscribed, async (track, publication, participant) => {
        if (track.kind === LivekitClient.Track.Kind.Audio) { track.detach().forEach(el => el.remove()); document.getElementById(`audio-${track.sid}`)?.remove(); syncWhisperAudioTracks(); }
        else if (publication.source === LivekitClient.Track.Source.ScreenShare) { track.detach(screenStageVideo); await hideScreenStage(); }
        else if (publication.source === LivekitClient.Track.Source.Camera) { const tile = document.getElementById(`tile-${participant.identity}`); if (tile) { track.detach(tile.querySelector("video")); setTilePlaceholder(participant.identity, false, "Camera Off"); } }
        scheduleParticipantsRender();
      });
      lkRoom.on(LivekitClient.RoomEvent.TrackPublished, scheduleParticipantsRender);
      lkRoom.on(LivekitClient.RoomEvent.TrackUnpublished, scheduleParticipantsRender);
      lkRoom.on(LivekitClient.RoomEvent.TrackMuted, (pub, p) => { if (pub.source === LivekitClient.Track.Source.Camera && p.identity !== clientId) setTilePlaceholder(p.identity, false, "Camera Off"); scheduleParticipantsRender(); });
      lkRoom.on(LivekitClient.RoomEvent.TrackUnmuted, (pub, p) => { if (pub.source === LivekitClient.Track.Source.Camera && p.identity !== clientId) setTilePlaceholder(p.identity, true, "Camera Off"); scheduleParticipantsRender(); });
      lkRoom.on(LivekitClient.RoomEvent.LocalTrackPublished, (pub) => {
        if (pub.source === LivekitClient.Track.Source.Camera && pub.track) { pub.track.attach(ensureVideoTile("local", `${getDisplayName()} (You)`, true, localAvatar).querySelector("video")); setTilePlaceholder("local", true, "Camera Disabled"); }
        else if (pub.source === LivekitClient.Track.Source.Microphone && pub.track?.mediaStreamTrack) { startLocalVoiceMonitor(pub.track.mediaStreamTrack); attachAudioTrackToRecorder(pub.track.mediaStreamTrack); syncWhisperAudioTracks(); }
        else if (pub.source === LivekitClient.Track.Source.ScreenShare && pub.track?.mediaStreamTrack) pub.track.mediaStreamTrack.contentHint = "detail";
        scheduleParticipantsRender();
      });
      lkRoom.on(LivekitClient.RoomEvent.LocalTrackUnpublished, async (pub) => {
        if (pub.source === LivekitClient.Track.Source.ScreenShare && isSharingScreen) { isSharingScreen = false; sendSignal({ type: "screen_share_stopped" }); await hideScreenStage(); updateControlButtons(); }
        scheduleParticipantsRender();
      });

      try { await lkRoom.connect(url, token); }
      catch (err) { showNoticeBanner(`❌ LiveKit error (${url}): ${err?.message || err}`, "denied", 0); return; }

      const lv = document.getElementById("tile-local")?.querySelector("video");
      if (lv?.srcObject) { lv.srcObject.getTracks().forEach(t => t.stop()); lv.srcObject = null; }
      if (wantMicOnJoin) await enableMicrophone();
      if (wantCamOnJoin) await enableCamera();

      for (const participant of lkRoom.remoteParticipants.values()) {
        const pid = participant.identity, pName = participant.name || peerNames[pid] || `User ${pid}`, pAvatar = peerAvatars[pid] || "";
        peerNames[pid] = pName; ensureVideoTile(pid, pName, true, pAvatar);
        for (const pub of participant.trackPublications.values()) {
          if (pub.isSubscribed && pub.track) {
            if (pub.track.kind === LivekitClient.Track.Kind.Audio) attachRemoteAudio(pub.track);
            else if (pub.source === LivekitClient.Track.Source.ScreenShare) showRemoteScreenStage(pName, pub.track, pub);
            else if (pub.source === LivekitClient.Track.Source.Camera) { pub.track.attach(ensureVideoTile(pid, pName, true, pAvatar).querySelector("video")); setTilePlaceholder(pid, !pub.isMuted, "Camera Off"); }
          }
        }
      }
      pruneGhostTiles(); applySubscriptionEfficiency(); refreshDmDropdown(); renderParticipantsList();
      if (isHost && currentHostId === clientId && autoRecordOnJoin && !isRecordingLocally) setTimeout(() => { if (hasJoinedRoom && isHost && currentHostId === clientId && !isRecordingLocally) startHostRecording(); }, 400);
    }

    function updateControlButtons() {
      const strictHost = Boolean(isHost && currentHostId === clientId);
      useMicBtn.classList.toggle("selected-mic", micEnabled); useSpeakerBtn.classList.toggle("selected-speaker", !micEnabled);
      muteSpeakerBtn.innerHTML = speakerMuted ? '🔇 <span class="hide-sm">Speaker Muted</span>' : '🔊 <span class="hide-sm">Mute Speaker</span>';
      muteSpeakerBtn.classList.toggle("danger", speakerMuted);
      muteSpeakerBtn.classList.toggle("secondary", !speakerMuted);
      camBtn.disabled = false; camBtn.innerHTML = camEnabled ? '📷 <span class="hide-sm">Disable </span>Cam<span class="hide-sm">era</span>' : '📷 <span class="hide-sm">Enable </span>Cam<span class="hide-sm">era</span>';
      camBtn.classList.toggle("active", camEnabled); camBtn.classList.toggle("secondary", !camEnabled);
      modalCamBtn.textContent = camEnabled ? "📷 Disable Camera" : "📷 Enable Camera"; modalCamBtn.classList.toggle("active", camEnabled); modalCamBtn.classList.toggle("secondary", !camEnabled);
      ccBtn.textContent = captionsEnabled ? "💬 CC: On" : "💬 CC: Off"; ccBtn.classList.toggle("active", captionsEnabled); ccBtn.classList.toggle("secondary", !captionsEnabled);
      hostBadge.classList.toggle("hidden", !strictHost); hostRecordBtn.classList.toggle("hidden", !strictHost);
      extendTimeBtn.classList.toggle("hidden", !(strictHost && meetingEndsAtMs));
      if (strictHost) { hostRecordBtn.textContent = isRecordingLocally ? "⏹ Stop Recording" : "🔴 Record"; hostRecordBtn.classList.toggle("danger", isRecordingLocally); hostRecordBtn.classList.toggle("secondary", !isRecordingLocally); }
      hostJoinPolicyBtn.classList.toggle("hidden", !strictHost);
      if (strictHost) { hostJoinPolicyBtn.textContent = requireJoinApproval ? "Entry: Owner Approval" : "Entry: Direct Join"; hostJoinPolicyBtn.classList.toggle("warning", requireJoinApproval); hostJoinPolicyBtn.classList.toggle("active", !requireJoinApproval); }
      hostPolicyBtn.classList.toggle("hidden", !strictHost);
      if (strictHost) { hostPolicyBtn.textContent = requireScreenApproval ? "Screen: Approval Req." : "Screen: Anyone"; hostPolicyBtn.classList.toggle("warning", requireScreenApproval); hostPolicyBtn.classList.toggle("active", !requireScreenApproval); }
      const someoneElseSharing = Boolean(activeSharer && activeSharer !== clientId);
      hostStopShareBtn.classList.toggle("hidden", !(strictHost && someoneElseSharing));

      screenBtn.classList.remove("active", "warning", "hidden");
      if (!navigator.mediaDevices?.getDisplayMedia) screenBtn.classList.add("hidden");
      else if (isSharingScreen) { screenBtn.disabled = false; screenBtn.textContent = "Stop Sharing"; screenBtn.classList.add("active"); }
      else if (someoneElseSharing) { screenBtn.disabled = true; screenBtn.textContent = `🔒 ${activeSharerName || "Someone"} is sharing`; document.getElementById("approved-banner")?.remove(); }
      else if (screenRequestPending) { screenBtn.disabled = true; screenBtn.textContent = "Waiting for Host..."; }
      else if (requireScreenApproval && !strictHost && !screenShareApproved) { screenBtn.disabled = false; screenBtn.textContent = "Request Screen Share"; screenBtn.classList.add("warning"); }
      else if (screenShareApproved) { screenBtn.disabled = false; screenBtn.textContent = "Approved — Share Screen"; screenBtn.classList.add("active"); }
      else { screenBtn.disabled = false; screenBtn.textContent = "Share Screen"; }
      updateWhiteboardUI(); scheduleParticipantsRender();
    }

    hostRecordBtn.onclick = async () => { if (!isHost || currentHostId !== clientId) return; controlsDock.classList.remove("more-open"); if (isRecordingLocally) await stopHostRecording(true); else await startHostRecording(); };
    hostJoinPolicyBtn.onclick = () => { if (isHost) sendSignal({ type: "set_join_policy", require_approval: !requireJoinApproval }); };
    useMicBtn.onclick = async () => { await enableMicrophone(); };
    useSpeakerBtn.onclick = async () => { await useSpeakerOnly(); showNoticeBanner("🔊 Switched to Computer Speaker Only (microphone off).", "", 2500); };
    muteSpeakerBtn.onclick = () => {
      speakerMuted = !speakerMuted;
      applySpeakerMuteState();
      updateControlButtons();
      showNoticeBanner(speakerMuted ? "🔇 Speaker muted (incoming audio silenced)." : "🔊 Speaker unmuted.", "", 2500);
    };
    camBtn.onclick = async () => { if (camEnabled) await disableCamera(); else await enableCamera(); };
    modalCamBtn.onclick = async () => { if (camEnabled) await disableCamera(); else await enableCamera(); };
    modalMicBtn.onclick = async () => { await enableMicrophone(); joinOptionsOverlay.classList.add("hidden"); connectToRoomSocket(false); };
    modalSpeakerBtn.onclick = async () => { await useSpeakerOnly(); joinOptionsOverlay.classList.add("hidden"); connectToRoomSocket(false); };
    cancelWaitingBtn.onclick = () => { leaveBtn.click(); };
    hostPolicyBtn.onclick = () => { if (isHost) sendSignal({ type: "set_screen_policy", require_approval: !requireScreenApproval }); };
    hostStopShareBtn.onclick = () => { if (isHost) sendSignal({ type: "host_stop_share" }); controlsDock.classList.remove("more-open"); };

    screenBtn.onclick = async () => {
      controlsDock.classList.remove("more-open");
      if (isSharingScreen) { await stopScreenShare(true); return; }
      if (activeSharer && activeSharer !== clientId) { showNoticeBanner(`${activeSharerName || "Another user"} is currently sharing.`, "denied"); return; }
      if (requireScreenApproval && !isHost && !screenShareApproved) {
        screenRequestPending = true; updateControlButtons();
        if (!ws || ws.readyState !== WebSocket.OPEN) connectToRoomSocket(true);
        sendRoomData({ type: "screen_share_request" }, currentHostId || null);
        showNoticeBanner("Screen share request sent to the host...", "", 3000);
        return;
      }
      document.getElementById("approved-banner")?.remove();
      await launchDisplayMedia();
    };

    function sendSignal(payload) { if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify(payload)); }

    function enterMeetingScreen(roomId, title) {
      currentRoomId = roomId; currentMeetingTitle = title || `Room ${roomId}`;
      isLeavingCall = false; wantMicOnJoin = false; wantCamOnJoin = false; micEnabled = false; camEnabled = false; speakerMuted = false; hasJoinedRoom = false; rejoinToken = "";
      waitingUsers = {}; pendingScreenRequests = {}; waitingCount = 0; currentHostId = null;
      syncMeetingCountdown(null, null);
      pinnedTileId = null; lastActiveSpeakerTileId = null; lastSpeakingKey = ""; lkSpeakingIds.clear(); seenMsgIds.clear();
      wbOpen = false; wbLocked = false; wbHistory = []; redrawWhiteboard();
      videoGrid.innerHTML = ""; tileOrder = ["local"]; currentPage = 1;
      renderAvatarPreviews(); lobbyView.classList.add("hidden"); callView.classList.remove("hidden");
      callTitleText.textContent = currentMeetingTitle; callSub.textContent = `Code: ${roomId} • Link: ${getMeetingUrl(roomId)}`;
      joinModalTitle.textContent = `Join "${currentMeetingTitle}"`;
      refreshDmDropdown(); ensureVideoTile("local", `${getDisplayName()} (You)`, true, localAvatar);
      setTilePlaceholder("local", false, "Camera Disabled"); applyViewMode(currentViewMode); updateControlButtons();
      joinOptionsOverlay.classList.remove("hidden");
    }

    function connectToRoomSocket(isReconnect = false) {
      if (hasJoinedRoom && !isReconnect) return;
      hasJoinedRoom = true; clearTimeout(wsReconnectTimer); clearInterval(wsPingTimer);
      const hostKey = localStorage.getItem(`vc_host_key_${currentRoomId}`) || sessionStorage.getItem(`vc_host_key_${currentRoomId}`) || "";
      const protocol = location.protocol === "https:" ? "wss:" : "ws:";
      const wsUrl = `${protocol}//${location.host}/ws/${encodeURIComponent(currentRoomId)}/${clientId}?name=${encodeURIComponent(getDisplayName())}&host_key=${encodeURIComponent(hostKey)}`;
      const sock = new WebSocket(wsUrl); ws = sock;

      sock.onopen = () => {
        clearInterval(wsPingTimer);
        wsPingTimer = setInterval(() => { if (ws === sock && sock.readyState === WebSocket.OPEN) sock.send(JSON.stringify({ type: "ping" })); }, 20000);
        sock.send(JSON.stringify({ type: "join_room", name: getDisplayName(), avatar: localAvatar, rejoin_token: isReconnect ? rejoinToken : "" }));
      };
      sock.onclose = () => {
        if (ws === sock) {
          clearInterval(wsPingTimer); wsPingTimer = null;
          if (hasJoinedRoom && !isLeavingCall) wsReconnectTimer = setTimeout(() => connectToRoomSocket(true), 1500);
        }
      };

      sock.onmessage = async (event) => {
        const msg = JSON.parse(event.data);
        if (msg.type === "pong") return;
        if (msg.type === "room_full") { isLeavingCall = true; alert(msg.reason || "Room is full."); leaveBtn.click(); return; }
        if (msg.type === "waiting_room") { waitingRoomOverlay.classList.remove("hidden"); waitingRoomStatus.textContent = msg.host_present ? "Waiting for the meeting owner to let you in..." : "Waiting for the meeting owner to join and let you in..."; return; }
        if (msg.type === "join_denied") { isLeavingCall = true; const r = msg.reason || "The meeting owner declined your request to join."; await leaveBtn.onclick(); showNoticeBanner(`❌ ${r}`, "denied", 5000); alert(r); return; }
        if (msg.type === "meeting_ended") { isLeavingCall = true; const r = msg.reason || "Meeting time limit reached. The meeting has ended."; await leaveBtn.onclick(); showNoticeBanner(`⏰ ${r}`, "denied", 6000); alert(`⏰ ${r}`); return; }
        if (msg.type === "meeting_time_warning") { showTimeWarningBanner(msg.minutes_left || 1); return; }
        if (msg.type === "meeting_extended") { showNoticeBanner(`⏱️ ${msg.by || "Host"} extended the meeting by ${msg.minutes || 15} minutes.`, "approved", 4000); return; }

        if (msg.type === "waiting_user_joined") {
          if (isHost) { waitingUsers[msg.user.id] = msg.user; waitingCount = Object.keys(waitingUsers).length; playWaitingKnockSound(); showWaitingUserBanner(msg.user); scheduleParticipantsRender(); }
        } else if (msg.type === "waiting_list") {
          if (isHost && msg.users?.length) { msg.users.forEach(u => { waitingUsers[u.id] = u; showWaitingUserBanner(u); }); waitingCount = Object.keys(waitingUsers).length; playWaitingKnockSound(); scheduleParticipantsRender(); }
        } else if (msg.type === "waiting_user_left") {
          delete waitingUsers[msg.sender]; waitingCount = Object.keys(waitingUsers).length; document.getElementById(`wait-${msg.sender}`)?.remove(); scheduleParticipantsRender();
        } else if (msg.type === "waiting_count_update") {
          waitingCount = msg.waiting_count || 0; scheduleParticipantsRender();
        } else if (msg.type === "all_users") {
          waitingRoomOverlay.classList.add("hidden");
          if (msg.rejoin_token) rejoinToken = msg.rejoin_token;
          isHost = Boolean(msg.is_host); currentHostId = msg.host_id || null;
          if (isHost && msg.host_secret) { try { sessionStorage.setItem(`vc_host_key_${currentRoomId}`, msg.host_secret); } catch {} }
          requireScreenApproval = Boolean(msg.require_screen_approval); requireJoinApproval = Boolean(msg.require_join_approval); autoRecordOnJoin = Boolean(msg.auto_record);
          activeSharer = msg.active_sharer || null; activeSharerName = msg.active_sharer_name || null; waitingCount = msg.waiting_count || 0; maxUsersInRoom = msg.max_users || 100;
          syncMeetingCountdown(msg.ends_at || null, msg.server_now || null);
          wbOpen = Boolean(msg.whiteboard_open); wbLocked = Boolean(msg.whiteboard_locked); wbHistory = Array.isArray(msg.whiteboard_strokes) ? msg.whiteboard_strokes : []; redrawWhiteboard();
          updateRecordingUI(Boolean(msg.is_recording), msg.recording_by || "Host");
          if (msg.is_recording && !isReconnect) showNoticeBanner("🔴 Notice: This meeting is currently being recorded by the host.", "denied", 6000);

          const validIds = new Set(msg.users.map(u => u.id));
          for (const tid of [...tileOrder]) { if (tid !== "local" && !validIds.has(tid) && !lkRoom?.remoteParticipants?.has(tid)) { delete peerNames[tid]; delete peerAvatars[tid]; removeVideoTile(tid); } }
          for (const user of msg.users) { peerNames[user.id] = user.name; peerAvatars[user.id] = user.avatar || ""; ensureVideoTile(user.id, user.name, true, user.avatar || ""); }
          refreshDmDropdown(); updateControlButtons();
          if (msg.livekit_url && msg.livekit_token && (!lkRoom || lkRoom.state === "disconnected")) await connectLiveKit(msg.livekit_url, msg.livekit_token);
        } else if (msg.type === "room_policy") {
          isHost = msg.host_id === clientId; currentHostId = msg.host_id || null;
          requireScreenApproval = Boolean(msg.require_screen_approval); requireJoinApproval = Boolean(msg.require_join_approval);
          activeSharer = msg.active_sharer || null; activeSharerName = msg.active_sharer_name || null; waitingCount = msg.waiting_count ?? waitingCount;
          if ("ends_at" in msg) syncMeetingCountdown(msg.ends_at || null, msg.server_now || null);
          if (typeof msg.whiteboard_open === "boolean") wbOpen = msg.whiteboard_open;
          if (typeof msg.whiteboard_locked === "boolean") wbLocked = msg.whiteboard_locked;
          updateRecordingUI(Boolean(msg.is_recording), msg.recording_by || "Host");
          if (!requireScreenApproval) { screenRequestPending = false; screenShareApproved = false; pendingScreenRequests = {}; }
          updateControlButtons();
        } else if (msg.type === "wb_toggle") { wbOpen = Boolean(msg.open); updateWhiteboardUI(); if (wbOpen) { redrawWhiteboard(); showNoticeBanner(`🎨 ${msg.by || "Someone"} opened the whiteboard.`, "", 2500); } }
        else if (msg.type === "wb_lock") { wbLocked = Boolean(msg.locked); updateWhiteboardUI(); }
        else if (msg.type === "wb_live") { drawSingleStroke(wbCtx, { tool: msg.tool, color: msg.color, size: msg.size, points: [[msg.x1, msg.y1], [msg.x2, msg.y2]] }); }
        else if (msg.type === "wb_commit") { if (msg.stroke) { wbHistory.push(msg.stroke); redrawWhiteboard(); } }
        else if (msg.type === "wb_undo") { wbHistory = wbHistory.filter(s => s.id !== msg.stroke_id); redrawWhiteboard(); }
        else if (msg.type === "wb_clear") { wbHistory = []; redrawWhiteboard(); showNoticeBanner(`🗑️ ${msg.by || "Someone"} cleared the whiteboard.`, "", 2200); }
        else if (msg.type === "recording_status") {
          updateRecordingUI(Boolean(msg.is_recording), msg.by || "Host");
          showNoticeBanner(msg.is_recording ? `🔴 Notice: ${msg.by || "The host"} started recording this meeting.` : "⏹ The host stopped recording this meeting.", msg.is_recording ? "denied" : "", msg.is_recording ? 6000 : 4000);
        } else if (msg.type === "screen_share_revoked") {
          if (isSharingScreen) await stopScreenShare(false);
          screenRequestPending = false; screenShareApproved = false; updateControlButtons();
          showNoticeBanner(msg.reason || "Screen share stopped.", "denied", 4500);
        } else if (["screen_share_request", "screen_share_approved", "screen_share_denied", "caption", "reaction", "chat_public", "chat_private"].includes(msg.type)) {
          handleIncomingRoomEvent(msg);
        } else if (msg.type === "user_joined") {
          const isNew = !peerNames[msg.user.id];
          peerNames[msg.user.id] = msg.user.name; peerAvatars[msg.user.id] = msg.user.avatar || "";
          ensureVideoTile(msg.user.id, msg.user.name, true, msg.user.avatar || "");
          scheduleDmRefresh();
          if (isNew) { playJoinSound(); showNoticeBanner(`👋 ${msg.user.name} joined the meeting.`, "", 2800); }
        } else if (msg.type === "user_left") {
          delete peerNames[msg.sender]; delete peerAvatars[msg.sender]; delete pendingScreenRequests[msg.sender]; lkSpeakingIds.delete(msg.sender);
          document.getElementById(`req-${msg.sender}`)?.remove();
          removeVideoTile(msg.sender); scheduleDmRefresh();
        }
      };
      updateControlButtons();
    }

    copyCallLinkBtn.onclick = () => copyText(getMeetingUrl(currentRoomId), copyCallLinkBtn);
    leaveBtn.onclick = async () => {
      isLeavingCall = true; clearTimeout(wsReconnectTimer); clearInterval(wsPingTimer); wsPingTimer = null;
      syncMeetingCountdown(null, null);
      clearTimeout(participantsRenderTimer); participantsRenderTimer = null; clearTimeout(dmRefreshTimer); dmRefreshTimer = null;
      if (isRecordingLocally) await stopHostRecording(true);
      stopLocalVoiceMonitor(); stopSpeechRecognition(); stopWhisperCaptions(); captionsEnabled = false; clearTimeout(captionClearTimer); captionBar.classList.add("hidden");
      await exitScreenFullscreenIfActive();
      if (document.fullscreenElement === whiteboardStage || document.webkitFullscreenElement === whiteboardStage) { try { if (document.exitFullscreen) await document.exitFullscreen(); else if (document.webkitExitFullscreen) await document.webkitExitFullscreen(); } catch {} }
      if (ws) try { ws.close(); } catch {} ws = null;
      if (lkRoom) { try { await lkRoom.disconnect(); } catch {} lkRoom = null; }
      const lv = document.getElementById("tile-local")?.querySelector("video");
      if (lv?.srcObject) { lv.srcObject.getTracks().forEach(t => t.stop()); lv.srcObject = null; }
      peerNames = {}; peerAvatars = {}; waitingUsers = {}; pendingScreenRequests = {}; waitingCount = 0; currentHostId = null; rejoinToken = "";
      pinnedTileId = null; lastActiveSpeakerTileId = null; lastSpeakingKey = ""; lkSpeakingIds.clear(); seenMsgIds.clear();
      wbOpen = false; wbLocked = false; wbHistory = []; redrawWhiteboard(); tileOrder = ["local"]; currentPage = 1;
      wantMicOnJoin = false; wantCamOnJoin = false; micEnabled = false; camEnabled = false; speakerMuted = false; isSharingScreen = false; hasJoinedRoom = false;
      isHost = false; autoRecordOnJoin = false; isMeetingRecording = false; requireJoinApproval = false; activeSharer = null; activeSharerName = null; screenRequestPending = false; screenShareApproved = false;
      audioContainer.innerHTML = ""; await hideScreenStage(); whiteboardStage.classList.add("hidden"); videoGrid.innerHTML = ""; reactionLayer.innerHTML = ""; bannerContainer.innerHTML = ""; publicMessages.innerHTML = ""; privateMessages.innerHTML = ""; participantSearch.value = "";
      controlsDock.classList.remove("more-open"); participantsPanel.classList.add("hidden");
      if (window.innerWidth <= 768) chatPanel.classList.add("hidden");
      joinOptionsOverlay.classList.add("hidden"); waitingRoomOverlay.classList.add("hidden"); recordingPill.classList.add("hidden");
      unreadPublic = 0; unreadPrivate = 0; updateUnreadBadges(); switchChatTab("public");
      callView.classList.add("hidden"); lobbyView.classList.remove("hidden");
      checkUrlForRoom(); if (isMainHostServer) { loadMeetings(); loadRecordings(); }
    };

    function handlePageExit() {
      isLeavingCall = true; clearTimeout(wsReconnectTimer); clearInterval(wsPingTimer);
      if (ws) try { ws.close(); } catch {}
      if (lkRoom) try { lkRoom.disconnect(); } catch {}
    }
    window.addEventListener("beforeunload", handlePageExit);
    window.addEventListener("pagehide", handlePageExit);
    window.addEventListener("popstate", checkUrlForRoom);
    checkOwnerAccess();
    checkUrlForRoom();
  </script>
</body>
</html>"""


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meetings (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                host_name TEXT NOT NULL,
                scheduled_at TEXT,
                created_at TEXT NOT NULL,
                host_key TEXT,
                require_screen_approval INTEGER NOT NULL DEFAULT 1,
                record_meeting INTEGER NOT NULL DEFAULT 0,
                require_join_approval INTEGER NOT NULL DEFAULT 0,
                duration_minutes INTEGER NOT NULL DEFAULT 0
            )
        """)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(meetings)").fetchall()}
        if "host_key" not in cols:
            conn.execute("ALTER TABLE meetings ADD COLUMN host_key TEXT")
        if "require_screen_approval" not in cols:
            conn.execute("ALTER TABLE meetings ADD COLUMN require_screen_approval INTEGER NOT NULL DEFAULT 1")
        if "record_meeting" not in cols:
            conn.execute("ALTER TABLE meetings ADD COLUMN record_meeting INTEGER NOT NULL DEFAULT 0")
        if "require_join_approval" not in cols:
            conn.execute("ALTER TABLE meetings ADD COLUMN require_join_approval INTEGER NOT NULL DEFAULT 0")
        if "duration_minutes" not in cols:
            conn.execute("ALTER TABLE meetings ADD COLUMN duration_minutes INTEGER NOT NULL DEFAULT 0")


init_db()


def is_main_host_request(request: Request) -> bool:
    client_ip = request.client.host if request.client else ""
    if client_ip not in ("127.0.0.1", "::1"):
        return False
    for hdr in ("x-forwarded-for", "x-real-ip", "forwarded", "cf-connecting-ip", "x-forwarded-host"):
        if request.headers.get(hdr):
            return False
    host_hdr = (request.headers.get("host") or "").split(":")[0].lower()
    return host_hdr in ("localhost", "127.0.0.1", "[::1]")


def require_main_host(request: Request):
    if not is_main_host_request(request):
        raise HTTPException(status_code=403, detail="Only the owner on the main host server can access this.")


def generate_meeting_code() -> str:
    alphabet = string.ascii_lowercase + string.digits
    return f"{''.join(secrets.choice(alphabet) for _ in range(3))}-{''.join(secrets.choice(alphabet) for _ in range(4))}-{''.join(secrets.choice(alphabet) for _ in range(3))}"


def get_meeting_row(meeting_id: str) -> dict | None:
    with get_db() as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM meetings WHERE id = ?", (meeting_id,)).fetchone()
    return dict(row) if row else None


def upsert_meeting_policy(room_id: str, meta: dict):
    with get_db() as conn:
        existing = conn.execute("SELECT id FROM meetings WHERE id = ?", (room_id,)).fetchone()
        if existing:
            conn.execute(
                "UPDATE meetings SET require_join_approval = ?, require_screen_approval = ?, host_key = COALESCE(host_key, ?) WHERE id = ?",
                (int(meta["require_join_approval"]), int(meta["require_screen_approval"]), meta.get("host_secret"), room_id),
            )
        else:
            conn.execute(
                "INSERT INTO meetings (id, title, host_name, scheduled_at, created_at, host_key, require_screen_approval, record_meeting, require_join_approval, duration_minutes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    room_id, f"Room {room_id}", "Host", None, datetime.now(timezone.utc).isoformat(),
                    meta.get("host_secret"), int(meta["require_screen_approval"]), int(meta["auto_record"]),
                    int(meta["require_join_approval"]), int(meta.get("duration_minutes", 0)),
                ),
            )


def sanitize_avatar(avatar: str | None) -> str:
    if not isinstance(avatar, str):
        return ""
    if avatar.startswith("data:image/") and len(avatar) <= 30_000:
        return avatar
    return ""


def mint_livekit_token(room_id: str, client_id: str, name: str, is_host: bool) -> str:
    return (
        lk_api.AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
        .with_identity(client_id)
        .with_name(name)
        .with_grants(lk_api.VideoGrants(room_join=True, room=room_id, can_publish=True, can_subscribe=True, can_publish_data=True, room_admin=is_host))
        .to_jwt()
    )


class MeetingCreate(BaseModel):
    title: str = "Instant Meeting"
    host_name: str = "Host"
    scheduled_at: str | None = None
    require_screen_approval: bool = True
    record_meeting: bool = False
    require_join_approval: bool = False
    duration_minutes: int = 0


@app.get("/")
@app.get("/room/{room_id}")
async def serve_app(room_id: str | None = None):
    return HTMLResponse(
        HTML_CONTENT,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
        },
    )


@app.get("/api/owner-status")
async def owner_status(request: Request):
    return {"is_main_host": is_main_host_request(request)}


@app.post("/api/meetings")
async def create_meeting(payload: MeetingCreate):
    meeting_id = generate_meeting_code()
    host_key = secrets.token_urlsafe(16)
    created_at = datetime.now(timezone.utc).isoformat()
    title = payload.title.strip() or "Meeting"
    host_name = payload.host_name.strip() or "Host"
    duration_minutes = max(0, min(1440, int(payload.duration_minutes or 0)))
    with get_db() as conn:
        conn.execute(
            "INSERT INTO meetings (id, title, host_name, scheduled_at, created_at, host_key, require_screen_approval, record_meeting, require_join_approval, duration_minutes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (meeting_id, title, host_name, payload.scheduled_at, created_at, host_key, int(payload.require_screen_approval), int(payload.record_meeting), int(payload.require_join_approval), duration_minutes),
        )
    return {
        "id": meeting_id, "title": title, "host_name": host_name, "scheduled_at": payload.scheduled_at, "created_at": created_at,
        "host_key": host_key, "require_screen_approval": payload.require_screen_approval, "record_meeting": payload.record_meeting,
        "require_join_approval": payload.require_join_approval, "duration_minutes": duration_minutes, "link": f"/room/{meeting_id}",
    }


@app.get("/api/meetings")
async def list_meetings(request: Request):
    require_main_host(request)
    with get_db() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT id, title, host_name, scheduled_at, created_at, require_screen_approval, record_meeting, require_join_approval, duration_minutes FROM meetings ORDER BY COALESCE(scheduled_at, created_at) ASC").fetchall()
    return [{**dict(r), "require_screen_approval": bool(r["require_screen_approval"]), "record_meeting": bool(r["record_meeting"]), "require_join_approval": bool(r["require_join_approval"]), "duration_minutes": int(r["duration_minutes"] or 0)} for r in rows]


@app.get("/api/meetings/{meeting_id}")
async def get_meeting(meeting_id: str):
    row = get_meeting_row(meeting_id)
    if not row:
        return {"id": meeting_id, "title": f"Room {meeting_id}", "host_name": None, "scheduled_at": None, "require_screen_approval": True, "record_meeting": False, "require_join_approval": False, "duration_minutes": 0}
    return {"id": row["id"], "title": row["title"], "host_name": row["host_name"], "scheduled_at": row["scheduled_at"], "require_screen_approval": bool(row["require_screen_approval"]), "record_meeting": bool(row["record_meeting"]), "require_join_approval": bool(row["require_join_approval"]), "duration_minutes": int(row["duration_minutes"] or 0)}


@app.delete("/api/meetings/{meeting_id}")
async def delete_meeting(meeting_id: str, request: Request):
    require_main_host(request)
    with get_db() as conn:
        if conn.execute("DELETE FROM meetings WHERE id = ?", (meeting_id,)).rowcount == 0:
            raise HTTPException(status_code=404, detail="Meeting not found")
    return {"deleted": True}


@app.post("/api/recordings/{room_id}")
async def save_recording(room_id: str, request: Request):
    req_host_key = request.headers.get("x-host-key", "")
    row = get_meeting_row(room_id)
    expected_secret = (row.get("host_key") if row else None) or room_meta.get(room_id, {}).get("host_secret")
    if not (expected_secret and req_host_key and secrets.compare_digest(req_host_key, expected_secret)):
        raise HTTPException(status_code=403, detail="Strictly only the meeting host can save recordings.")
    data = await request.body()
    if not data:
        raise HTTPException(status_code=400, detail="Empty recording")
    safe_room = "".join(c for c in room_id if c.isalnum() or c in "-_") or "meeting"
    filename = f"meeting-{safe_room}-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.webm"
    file_path = RECORDINGS_DIR / filename
    file_path.write_bytes(data)
    return {"saved": True, "filename": filename, "path": str(file_path), "url": f"/recordings/{filename}"}


@app.get("/api/recordings")
async def list_recordings(request: Request):
    require_main_host(request)
    files = sorted(RECORDINGS_DIR.glob("*.webm"), key=lambda p: p.stat().st_mtime, reverse=True)
    return [{"filename": f.name, "size_mb": round(f.stat().st_size / (1024 * 1024), 2), "url": f"/recordings/{f.name}"} for f in files]


def _parse_range_header(range_header: str, file_size: int) -> tuple[int, int]:
    try:
        unit, range_spec = range_header.strip().split("=", 1)
        if unit.lower() != "bytes":
            raise ValueError
        start_str, end_str = range_spec.split("-", 1)
        if not start_str and not end_str:
            raise ValueError
        start = max(0, file_size - int(end_str)) if not start_str else int(start_str)
        end = (file_size - 1) if (not start_str or not end_str) else int(end_str)
    except ValueError:
        raise HTTPException(status_code=416, detail="Invalid Range header")
    if start < 0 or end >= file_size or start > end:
        raise HTTPException(status_code=416, detail="Requested Range Not Satisfiable")
    return start, end


def _iter_file_range(file_path: Path, start: int, end: int, chunk_size: int = 256 * 1024):
    with open(file_path, "rb") as f:
        f.seek(start)
        remaining = end - start + 1
        while remaining > 0:
            chunk = f.read(min(chunk_size, remaining))
            if not chunk:
                break
            remaining -= len(chunk)
            yield chunk


@app.get("/recordings/{filename}")
async def get_recording_file(filename: str, request: Request, download: int = 0):
    require_main_host(request)
    safe_name = Path(filename).name
    file_path = RECORDINGS_DIR / safe_name
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Recording not found")
    if download:
        return FileResponse(file_path, media_type="video/webm", filename=safe_name)
    file_size = file_path.stat().st_size
    range_header = request.headers.get("range")
    headers = {"Accept-Ranges": "bytes"}
    if range_header:
        start, end = _parse_range_header(range_header, file_size)
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
        headers["Content-Length"] = str(end - start + 1)
        return StreamingResponse(_iter_file_range(file_path, start, end), status_code=206, headers=headers, media_type="video/webm")
    headers["Content-Length"] = str(file_size)
    return StreamingResponse(_iter_file_range(file_path, 0, file_size - 1), status_code=200, headers=headers, media_type="video/webm")


async def _safe_send(ws: WebSocket, text: str):
    try:
        await asyncio.wait_for(ws.send_text(text), timeout=3.0)
    except Exception:
        pass


async def broadcast_room(room_id: str, payload: dict, exclude_id: str | None = None):
    peers = list(rooms.get(room_id, {}).items())
    if not peers:
        return
    text = json.dumps(payload)
    tasks = [_safe_send(info["ws"], text) for uid, info in peers if uid != exclude_id]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def broadcast_waiting_count(room_id: str):
    await broadcast_room(room_id, {"type": "waiting_count_update", "waiting_count": len(waiting_rooms.get(room_id, {}))})


async def run_meeting_timer(room_id: str):
    try:
        warned_5m = False
        warned_1m = False
        while room_id in room_meta:
            meta = room_meta.get(room_id)
            if not meta or not meta.get("ends_at"):
                return
            remaining = (meta["ends_at"] - datetime.now(timezone.utc)).total_seconds()
            if remaining > 300:
                warned_5m = False
                warned_1m = False
            elif remaining > 60:
                warned_1m = False
            if remaining <= 0:
                end_msg = json.dumps({"type": "meeting_ended", "reason": "Meeting time limit reached. The meeting has ended."})
                for uid, info in list(rooms.get(room_id, {}).items()):
                    await _safe_send(info["ws"], end_msg)
                    try:
                        await info["ws"].close()
                    except Exception:
                        pass
                for wid, winfo in list(waiting_rooms.get(room_id, {}).items()):
                    await _safe_send(winfo["ws"], end_msg)
                    try:
                        await winfo["ws"].close()
                    except Exception:
                        pass
                return
            if remaining <= 300 and not warned_5m and meta.get("duration_minutes", 0) > 5:
                warned_5m = True
                await broadcast_room(room_id, {"type": "meeting_time_warning", "minutes_left": 5})
            elif remaining <= 60 and not warned_1m:
                warned_1m = True
                await broadcast_room(room_id, {"type": "meeting_time_warning", "minutes_left": 1})
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass


def build_policy_payload(room_id: str) -> dict:
    meta = room_meta[room_id]
    return {
        "type": "room_policy", "host_id": meta["host_id"], "require_screen_approval": meta["require_screen_approval"],
        "require_join_approval": meta["require_join_approval"], "active_sharer": meta["active_sharer"],
        "active_sharer_name": meta["active_sharer_name"], "is_recording": meta["is_recording"],
        "recording_by": meta["recording_by"], "waiting_count": len(waiting_rooms.get(room_id, {})),
        "whiteboard_open": meta["whiteboard_open"], "whiteboard_locked": meta["whiteboard_locked"],
        "duration_minutes": meta.get("duration_minutes", 0),
        "ends_at": meta["ends_at"].isoformat() if meta.get("ends_at") else None,
        "server_now": datetime.now(timezone.utc).isoformat(),
    }


async def admit_user_to_room(room_id: str, uid: str, ws: WebSocket, name: str, avatar: str, is_host: bool):
    meta = room_meta[room_id]
    existing_users = [{"id": eid, "name": info["name"], "avatar": info["avatar"]} for eid, info in rooms[room_id].items() if eid != uid]
    already_in_room = uid in rooms[room_id]
    if already_in_room and rooms[room_id][uid]["ws"] is not ws:
        try:
            await rooms[room_id][uid]["ws"].close()
        except Exception:
            pass
    rooms[room_id][uid] = {"ws": ws, "name": name, "avatar": avatar}
    rejoin_token = secrets.token_urlsafe(12)
    meta["rejoin_tokens"][uid] = rejoin_token
    lk_token = mint_livekit_token(room_id, uid, name, is_host)

    payload = {
        "type": "all_users", "users": existing_users, "is_host": is_host, "host_id": meta["host_id"],
        "require_screen_approval": meta["require_screen_approval"], "require_join_approval": meta["require_join_approval"],
        "auto_record": meta["auto_record"], "is_recording": meta["is_recording"], "recording_by": meta["recording_by"],
        "active_sharer": meta["active_sharer"], "active_sharer_name": meta["active_sharer_name"],
        "waiting_count": len(waiting_rooms.get(room_id, {})), "whiteboard_open": meta["whiteboard_open"],
        "whiteboard_locked": meta["whiteboard_locked"], "whiteboard_strokes": meta["whiteboard_strokes"],
        "duration_minutes": meta.get("duration_minutes", 0),
        "ends_at": meta["ends_at"].isoformat() if meta.get("ends_at") else None,
        "server_now": datetime.now(timezone.utc).isoformat(),
        "rejoin_token": rejoin_token,
        "livekit_url": LIVEKIT_URL, "livekit_token": lk_token, "max_users": MAX_USERS_PER_ROOM,
    }
    if is_host and meta.get("host_secret"):
        payload["host_secret"] = meta["host_secret"]
    await ws.send_text(json.dumps(payload))

    if not already_in_room:
        await broadcast_room(room_id, {"type": "user_joined", "user": {"id": uid, "name": name, "avatar": avatar}}, exclude_id=uid)

    if is_host:
        await broadcast_room(room_id, build_policy_payload(room_id), exclude_id=uid)
        if waiting_rooms.get(room_id):
            await ws.send_text(json.dumps({"type": "waiting_list", "users": [{"id": wid, "name": wi["name"], "avatar": wi["avatar"]} for wid, wi in waiting_rooms[room_id].items()]}))
        for req_id, req_name in list(meta.get("pending_screen_requests", {}).items()):
            await ws.send_text(json.dumps({"type": "screen_share_request", "sender": req_id, "sender_name": req_name}))


@app.websocket("/ws/{room_id}/{client_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str, client_id: str, name: str = "Guest", host_key: str = ""):
    await websocket.accept()
    meeting_row = get_meeting_row(room_id)
    if room_id not in rooms:
        duration_mins = int(meeting_row["duration_minutes"] or 0) if meeting_row else 0
        ends_at = (datetime.now(timezone.utc) + timedelta(minutes=duration_mins)) if duration_mins > 0 else None
        rooms[room_id] = {}
        waiting_rooms[room_id] = {}
        room_meta[room_id] = {
            "host_id": None,
            "host_secret": meeting_row.get("host_key") if meeting_row else None,
            "require_screen_approval": bool(meeting_row["require_screen_approval"]) if meeting_row else True,
            "require_join_approval": bool(meeting_row["require_join_approval"]) if meeting_row else False,
            "auto_record": bool(meeting_row["record_meeting"]) if meeting_row else False,
            "duration_minutes": duration_mins,
            "ends_at": ends_at,
            "timer_task": asyncio.create_task(run_meeting_timer(room_id)) if ends_at else None,
            "rejoin_tokens": {},
            "active_sharer": None, "active_sharer_name": None,
            "approved_sharers": set(), "pending_screen_requests": {},
            "is_recording": False, "recording_by": None,
            "whiteboard_open": False, "whiteboard_locked": False, "whiteboard_strokes": [],
            "is_ad_hoc": meeting_row is None,
        }

    meta = room_meta[room_id]
    user_name = name
    user_avatar = ""
    is_host = False
    joined_or_waiting = False

    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            msg_type = message.get("type")
            target = message.get("target")

            if msg_type == "ping":
                await websocket.send_text('{"type":"pong"}')
                continue

            if msg_type == "join_room" and not joined_or_waiting:
                joined_or_waiting = True
                user_name = (message.get("name") or name or "Guest").strip()[:60]
                user_avatar = sanitize_avatar(message.get("avatar"))

                if client_id not in rooms[room_id] and len(rooms[room_id]) >= MAX_USERS_PER_ROOM:
                    await websocket.send_text(json.dumps({"type": "room_full", "reason": f"Room is full (maximum {MAX_USERS_PER_ROOM} participants)."}))
                    await websocket.close()
                    return

                latest_row = get_meeting_row(room_id)
                expected_secret = (latest_row.get("host_key") if latest_row else None) or meta.get("host_secret")
                if expected_secret:
                    is_host = bool(host_key and secrets.compare_digest(host_key, expected_secret))
                else:
                    is_host = meta["host_id"] is None
                    if is_host:
                        meta["host_secret"] = secrets.token_urlsafe(16)

                if is_host:
                    meta["host_id"] = client_id

                req_rejoin_token = message.get("rejoin_token") or ""
                valid_rejoin = (
                    client_id in rooms[room_id]
                    and bool(req_rejoin_token)
                    and secrets.compare_digest(req_rejoin_token, meta["rejoin_tokens"].get(client_id, ""))
                )

                if is_host or not meta["require_join_approval"] or valid_rejoin:
                    await admit_user_to_room(room_id, client_id, websocket, user_name, user_avatar, is_host)
                else:
                    waiting_rooms[room_id][client_id] = {"ws": websocket, "name": user_name, "avatar": user_avatar}
                    await websocket.send_text(json.dumps({"type": "waiting_room", "title": latest_row["title"] if latest_row else f"Room {room_id}", "host_present": bool(meta["host_id"] and meta["host_id"] in rooms[room_id])}))
                    host_id = meta["host_id"]
                    if host_id and host_id in rooms[room_id]:
                        await _safe_send(rooms[room_id][host_id]["ws"], json.dumps({"type": "waiting_user_joined", "user": {"id": client_id, "name": user_name, "avatar": user_avatar}}))
                    await broadcast_waiting_count(room_id)

            elif msg_type == "join_decision":
                if client_id == meta["host_id"] and target in waiting_rooms.get(room_id, {}):
                    approved = bool(message.get("approved"))
                    w_info = waiting_rooms[room_id].pop(target)
                    if approved:
                        if len(rooms[room_id]) >= MAX_USERS_PER_ROOM:
                            await _safe_send(w_info["ws"], json.dumps({"type": "join_denied", "reason": f"Room is full (maximum {MAX_USERS_PER_ROOM})."}))
                            await w_info["ws"].close()
                        else:
                            await admit_user_to_room(room_id, target, w_info["ws"], w_info["name"], w_info["avatar"], False)
                    else:
                        await _safe_send(w_info["ws"], json.dumps({"type": "join_denied", "reason": "The meeting owner declined your request to join."}))
                        await w_info["ws"].close()
                    await broadcast_waiting_count(room_id)

            elif msg_type == "admit_all":
                if client_id == meta["host_id"] and waiting_rooms.get(room_id):
                    for wid, w_info in list(waiting_rooms[room_id].items()):
                        waiting_rooms[room_id].pop(wid, None)
                        if len(rooms[room_id]) < MAX_USERS_PER_ROOM:
                            await admit_user_to_room(room_id, wid, w_info["ws"], w_info["name"], w_info["avatar"], False)
                        else:
                            await _safe_send(w_info["ws"], json.dumps({"type": "join_denied", "reason": f"Room is full (maximum {MAX_USERS_PER_ROOM})."}))
                            await w_info["ws"].close()
                    await broadcast_waiting_count(room_id)

            elif msg_type == "set_join_policy":
                if client_id == meta["host_id"]:
                    new_val = bool(message.get("require_approval", False))
                    meta["require_join_approval"] = new_val
                    upsert_meeting_policy(room_id, meta)
                    if not new_val and waiting_rooms.get(room_id):
                        for wid, w_info in list(waiting_rooms[room_id].items()):
                            waiting_rooms[room_id].pop(wid, None)
                            if len(rooms[room_id]) < MAX_USERS_PER_ROOM:
                                await admit_user_to_room(room_id, wid, w_info["ws"], w_info["name"], w_info["avatar"], False)
                            else:
                                await _safe_send(w_info["ws"], json.dumps({"type": "join_denied", "reason": f"Room is full (maximum {MAX_USERS_PER_ROOM})."}))
                    await broadcast_room(room_id, build_policy_payload(room_id))

            elif msg_type == "extend_meeting":
                if client_id == meta["host_id"] and meta.get("ends_at"):
                    try:
                        add_mins = max(1, min(180, int(message.get("minutes", 15))))
                    except (TypeError, ValueError):
                        add_mins = 15
                    meta["ends_at"] = meta["ends_at"] + timedelta(minutes=add_mins)
                    await broadcast_room(room_id, build_policy_payload(room_id))
                    await broadcast_room(room_id, {"type": "meeting_extended", "minutes": add_mins, "by": user_name})

            elif client_id not in rooms.get(room_id, {}):
                continue

            elif msg_type == "wb_toggle":
                meta["whiteboard_open"] = bool(message.get("open", False))
                await broadcast_room(room_id, {"type": "wb_toggle", "open": meta["whiteboard_open"], "by": user_name}, exclude_id=client_id)

            elif msg_type == "wb_lock":
                if client_id == meta["host_id"]:
                    meta["whiteboard_locked"] = bool(message.get("locked", False))
                    await broadcast_room(room_id, {"type": "wb_lock", "locked": meta["whiteboard_locked"]})

            elif msg_type == "wb_live":
                if not meta["whiteboard_locked"] or client_id == meta["host_id"]:
                    message["sender"] = client_id
                    await broadcast_room(room_id, message, exclude_id=client_id)

            elif msg_type == "wb_commit":
                if not meta["whiteboard_locked"] or client_id == meta["host_id"]:
                    stroke = message.get("stroke")
                    if isinstance(stroke, dict):
                        stroke["sender"] = client_id
                        meta["whiteboard_strokes"].append(stroke)
                        if len(meta["whiteboard_strokes"]) > MAX_WB_STROKES:
                            meta["whiteboard_strokes"] = meta["whiteboard_strokes"][-MAX_WB_STROKES:]
                        await broadcast_room(room_id, {"type": "wb_commit", "stroke": stroke}, exclude_id=client_id)

            elif msg_type == "wb_undo":
                stroke_id = message.get("stroke_id")
                if stroke_id:
                    before = len(meta["whiteboard_strokes"])
                    meta["whiteboard_strokes"] = [s for s in meta["whiteboard_strokes"] if not (s.get("id") == stroke_id and (s.get("sender") == client_id or client_id == meta["host_id"]))]
                    if len(meta["whiteboard_strokes"]) != before:
                        await broadcast_room(room_id, {"type": "wb_undo", "stroke_id": stroke_id}, exclude_id=client_id)

            elif msg_type == "wb_clear":
                if not meta["whiteboard_locked"] or client_id == meta["host_id"]:
                    meta["whiteboard_strokes"].clear()
                    await broadcast_room(room_id, {"type": "wb_clear", "by": user_name}, exclude_id=client_id)

            elif msg_type in ("chat_public", "reaction", "caption"):
                message["sender"] = client_id
                message["sender_name"] = user_name
                await broadcast_room(room_id, message, exclude_id=client_id)

            elif msg_type == "chat_private":
                if target and target in rooms.get(room_id, {}):
                    message["sender"] = client_id
                    message["sender_name"] = user_name
                    await _safe_send(rooms[room_id][target]["ws"], json.dumps(message))

            elif msg_type == "recording_started":
                if client_id == meta["host_id"]:
                    meta["is_recording"] = True
                    meta["recording_by"] = user_name
                    await broadcast_room(room_id, {"type": "recording_status", "is_recording": True, "by": user_name})

            elif msg_type == "recording_stopped":
                if client_id == meta["host_id"]:
                    meta["is_recording"] = False
                    meta["recording_by"] = None
                    await broadcast_room(room_id, {"type": "recording_status", "is_recording": False, "by": user_name})

            elif msg_type == "set_screen_policy":
                if client_id == meta["host_id"]:
                    new_val = bool(message.get("require_approval", True))
                    meta["require_screen_approval"] = new_val
                    if not new_val:
                        meta["approved_sharers"].clear()
                        meta["pending_screen_requests"].clear()
                    upsert_meeting_policy(room_id, meta)
                    await broadcast_room(room_id, build_policy_payload(room_id))

            elif msg_type == "screen_share_request":
                if meta["active_sharer"] and meta["active_sharer"] != client_id:
                    await websocket.send_text(json.dumps({"type": "screen_share_denied", "reason": f"{meta['active_sharer_name']} is already sharing their screen."}))
                elif not meta["require_screen_approval"] or client_id == meta["host_id"]:
                    meta["approved_sharers"].add(client_id)
                    await websocket.send_text(json.dumps({"type": "screen_share_approved"}))
                else:
                    meta["pending_screen_requests"][client_id] = user_name
                    host_id = meta["host_id"]
                    if host_id and host_id in rooms.get(room_id, {}):
                        await _safe_send(rooms[room_id][host_id]["ws"], json.dumps({"type": "screen_share_request", "msgId": message.get("msgId"), "sender": client_id, "sender_name": user_name}))
                    elif not host_id:
                        meta["pending_screen_requests"].pop(client_id, None)
                        await websocket.send_text(json.dumps({"type": "screen_share_denied", "reason": "The meeting host is not in the room yet."}))

            elif msg_type == "screen_share_decision":
                if client_id == meta["host_id"] and target in rooms.get(room_id, {}):
                    meta["pending_screen_requests"].pop(target, None)
                    approved = bool(message.get("approved"))
                    if approved:
                        if meta["active_sharer"] and meta["active_sharer"] != target:
                            await _safe_send(rooms[room_id][target]["ws"], json.dumps({"type": "screen_share_denied", "reason": f"{meta['active_sharer_name']} is currently sharing."}))
                        else:
                            meta["approved_sharers"].add(target)
                            await _safe_send(rooms[room_id][target]["ws"], json.dumps({"type": "screen_share_approved"}))
                    else:
                        meta["approved_sharers"].discard(target)
                        await _safe_send(rooms[room_id][target]["ws"], json.dumps({"type": "screen_share_denied", "reason": "The meeting host declined your screen share request."}))

            elif msg_type == "screen_share_started":
                if meta["active_sharer"] and meta["active_sharer"] != client_id:
                    await websocket.send_text(json.dumps({"type": "screen_share_revoked", "reason": f"{meta['active_sharer_name']} is already sharing their screen."}))
                elif meta["require_screen_approval"] and client_id != meta["host_id"] and client_id not in meta["approved_sharers"]:
                    await websocket.send_text(json.dumps({"type": "screen_share_revoked", "reason": "Host approval is required to share your screen."}))
                else:
                    meta["approved_sharers"].discard(client_id)
                    meta["pending_screen_requests"].pop(client_id, None)
                    meta["active_sharer"] = client_id
                    meta["active_sharer_name"] = user_name
                    await broadcast_room(room_id, build_policy_payload(room_id))

            elif msg_type == "screen_share_stopped":
                if meta["active_sharer"] == client_id:
                    meta["active_sharer"] = None
                    meta["active_sharer_name"] = None
                    await broadcast_room(room_id, build_policy_payload(room_id))

            elif msg_type == "host_stop_share":
                if client_id == meta["host_id"] and meta["active_sharer"]:
                    sharer_id = meta["active_sharer"]
                    meta["active_sharer"] = None
                    meta["active_sharer_name"] = None
                    if sharer_id in rooms.get(room_id, {}):
                        await _safe_send(rooms[room_id][sharer_id]["ws"], json.dumps({"type": "screen_share_revoked", "reason": "The meeting host stopped your screen share."}))
                    await broadcast_room(room_id, build_policy_payload(room_id))

    except WebSocketDisconnect:
        if room_id in waiting_rooms and client_id in waiting_rooms[room_id] and waiting_rooms[room_id][client_id]["ws"] is websocket:
            del waiting_rooms[room_id][client_id]
            host_id = meta["host_id"]
            if host_id and host_id in rooms.get(room_id, {}):
                await _safe_send(rooms[room_id][host_id]["ws"], json.dumps({"type": "waiting_user_left", "sender": client_id}))
            await broadcast_waiting_count(room_id)

        if room_id in rooms and client_id in rooms[room_id] and rooms[room_id][client_id]["ws"] is websocket:
            del rooms[room_id][client_id]
            meta["rejoin_tokens"].pop(client_id, None)
            meta["approved_sharers"].discard(client_id)
            meta["pending_screen_requests"].pop(client_id, None)

            policy_changed = False
            recording_stopped = False
            if meta["active_sharer"] == client_id:
                meta["active_sharer"] = None
                meta["active_sharer_name"] = None
                policy_changed = True
            if meta["host_id"] == client_id:
                if meta["is_recording"]:
                    meta["is_recording"] = False
                    meta["recording_by"] = None
                    recording_stopped = True
                meta["host_id"] = None
                policy_changed = True

            await broadcast_room(room_id, {"type": "user_left", "sender": client_id})
            if recording_stopped and rooms[room_id]:
                await broadcast_room(room_id, {"type": "recording_status", "is_recording": False})
            if policy_changed and rooms[room_id]:
                await broadcast_room(room_id, build_policy_payload(room_id))

        if room_id in rooms and not rooms[room_id] and not waiting_rooms.get(room_id):
            timer_task = meta.get("timer_task")
            if timer_task and not timer_task.done():
                timer_task.cancel()
            rooms.pop(room_id, None)
            waiting_rooms.pop(room_id, None)
            room_meta.pop(room_id, None)
