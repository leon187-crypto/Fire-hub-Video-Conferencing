import json
import os
import secrets
import sqlite3
import string
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from livekit import api as lk_api
from pydantic import BaseModel

app = FastAPI()

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "meetings.db"
RECORDINGS_DIR = BASE_DIR / "recordings"
RECORDINGS_DIR.mkdir(exist_ok=True)

MAX_USERS_PER_ROOM = 100
MAX_WB_STROKES = 5000

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
  <title>Python LiveKit Video Conference (100 Users)</title>
  <script src="https://cdn.jsdelivr.net/npm/livekit-client/dist/livekit-client.umd.min.js"></script>
  <script src="https://cdn.jsdelivr.net/gh/yusitnikov/fix-webm-duration/fix-webm-duration.js"></script>
  <style>
    * { box-sizing: border-box; font-family: system-ui, -apple-system, sans-serif; }
    body {
      margin: 0;
      background: #0f1115;
      color: #f4f4f5;
      min-height: 100vh;
      min-height: 100dvh;
      display: flex;
      flex-direction: column;
    }
    .hidden { display: none !important; }
    .page-hidden { display: none !important; }
    .mobile-only { display: none !important; }

    /* Lobby & Scheduler */
    #lobby-view {
      max-width: 960px;
      width: 100%;
      margin: 2rem auto;
      padding: 0 1.25rem;
      display: grid;
      gap: 1.5rem;
    }
    #lobby-home-sections { display: grid; gap: 1.5rem; }
    .lobby-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 1rem;
    }
    .lobby-header h1 { margin: 0; font-size: 1.6rem; }

    /* Profile Widget */
    .profile-setup {
      display: flex;
      align-items: center;
      gap: 0.85rem;
      background: #181b22;
      border: 1px solid #2b303b;
      padding: 0.65rem 0.9rem;
      border-radius: 12px;
      flex-wrap: wrap;
    }
    .avatar-circle {
      width: 52px;
      height: 52px;
      border-radius: 50%;
      background: #2563eb;
      color: #fff;
      font-weight: 700;
      font-size: 1.15rem;
      display: flex;
      align-items: center;
      justify-content: center;
      overflow: hidden;
      flex-shrink: 0;
      border: 2px solid #3b82f6;
      transition: box-shadow 0.15s ease, border-color 0.15s ease, transform 0.15s ease;
    }
    .avatar-circle img { width: 100%; height: 100%; object-fit: cover; }
    .avatar-circle.large { width: 82px; height: 82px; font-size: 1.7rem; }
    .avatar-circle.small { width: 34px; height: 34px; font-size: 0.85rem; border-width: 1.5px; }

    .cards {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 1.25rem;
    }
    .card {
      background: #181b22;
      border: 1px solid #2b303b;
      border-radius: 12px;
      padding: 1.25rem;
      display: flex;
      flex-direction: column;
      gap: 0.85rem;
    }
    .card h2 { margin: 0; font-size: 1.1rem; color: #e4e4e7; }
    label { font-size: 0.85rem; color: #a1a1aa; display: block; margin-bottom: 0.3rem; }
    input[type="text"], input[type="datetime-local"], select {
      width: 100%;
      padding: 0.6rem 0.75rem;
      border-radius: 8px;
      border: 1px solid #333844;
      background: #101217;
      color: #fff;
      font-size: 0.95rem;
    }
    .checkbox-row {
      display: flex;
      align-items: center;
      gap: 0.55rem;
      font-size: 0.88rem;
      color: #d4d4d8;
      cursor: pointer;
      margin: 0;
    }
    .checkbox-row input { width: auto; accent-color: #2563eb; cursor: pointer; }
    button, .btn-link {
      padding: 0.55rem 0.9rem;
      border: none;
      border-radius: 8px;
      cursor: pointer;
      font-weight: 600;
      font-size: 0.88rem;
      background: #2563eb;
      color: #fff;
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 4px;
      touch-action: manipulation;
      transition: opacity 0.15s, background 0.15s;
    }
    button:hover:not(:disabled), .btn-link:hover { opacity: 0.9; }
    button:disabled { opacity: 0.45; cursor: not-allowed; }
    button.danger { background: #dc2626; }
    button.secondary, .btn-link.secondary { background: #303642; }
    button.active { background: #16a34a; }
    button.warning { background: #d97706; }
    .row { display: flex; gap: 0.5rem; flex-wrap: wrap; align-items: center; }

    .meeting-list { display: flex; flex-direction: column; gap: 0.75rem; }
    .meeting-item {
      background: #101217;
      border: 1px solid #2b303b;
      border-radius: 8px;
      padding: 0.9rem 1rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 0.75rem;
    }
    .meeting-meta { display: flex; flex-direction: column; gap: 0.2rem; min-width: 0; flex: 1; }
    .meeting-title { font-weight: 600; font-size: 1rem; word-break: break-word; }
    .meeting-sub { font-size: 0.82rem; color: #a1a1aa; word-break: break-word; }
    .link-box {
      background: #101217;
      border: 1px dashed #3b82f6;
      padding: 0.6rem 0.75rem;
      border-radius: 8px;
      font-size: 0.85rem;
      word-break: break-all;
      color: #93c5fd;
    }

    /* Recording Player Modal */
    #player-modal {
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, 0.85);
      z-index: 60;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 1rem;
    }
    .player-box {
      background: #181b22;
      border: 1px solid #333844;
      border-radius: 12px;
      padding: 1rem;
      width: min(95vw, 960px);
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
    }
    .player-header { display: flex; justify-content: space-between; align-items: center; gap: 0.75rem; }
    #player-video { width: 100%; max-height: 75dvh; background: #000; border-radius: 8px; }

    /* Call View */
    #call-view {
      display: flex;
      flex-direction: column;
      height: 100vh;
      height: 100dvh;
      overflow: hidden;
    }
    #call-header {
      order: 1;
      padding: 0.65rem 1.15rem;
      background: #181b22;
      border-bottom: 1px solid #2b303b;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 0.65rem;
      flex-shrink: 0;
    }
    .call-info { display: flex; flex-direction: column; min-width: 0; flex: 1; }
    .call-title { font-weight: 700; font-size: 1rem; display: flex; align-items: center; gap: 0.45rem; flex-wrap: wrap; }
    .host-pill {
      font-size: 0.7rem;
      background: #7c3aed;
      color: #fff;
      padding: 2px 7px;
      border-radius: 999px;
      font-weight: 600;
    }
    .count-pill {
      font-size: 0.75rem;
      background: #272c38;
      color: #e4e4e7;
      padding: 2px 8px;
      border-radius: 999px;
    }
    .joining-pill {
      font-size: 0.73rem;
      background: #b45309;
      color: #fff;
      padding: 2px 8px;
      border-radius: 999px;
      font-weight: 600;
    }
    .rec-pill {
      font-size: 0.72rem;
      background: #dc2626;
      color: #fff;
      padding: 2px 9px;
      border-radius: 999px;
      font-weight: 700;
      display: inline-flex;
      align-items: center;
      gap: 5px;
      animation: pulseRec 1.4s infinite;
    }
    @keyframes pulseRec { 0%, 100% { opacity: 1; } 50% { opacity: 0.55; } }
    .call-sub {
      font-size: 0.78rem;
      color: #a1a1aa;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .header-actions { display: flex; gap: 0.45rem; align-items: center; flex-shrink: 0; }

    /* Controls Dock */
    #controls-dock {
      order: 2;
      background: #14171d;
      border-bottom: 1px solid #2b303b;
      padding: 0.5rem 1.15rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 0.5rem;
      flex-shrink: 0;
      position: relative;
      z-index: 25;
    }
    .primary-controls, .secondary-controls {
      display: flex;
      gap: 0.45rem;
      flex-wrap: wrap;
      align-items: center;
    }

    #view-mode-select {
      width: auto;
      padding: 0.45rem 0.65rem;
      font-size: 0.84rem;
      font-weight: 600;
      background: #303642;
      border: 1px solid #454d5e;
      cursor: pointer;
    }

    .audio-switch {
      display: inline-flex;
      background: #101217;
      border: 1px solid #333844;
      border-radius: 8px;
      padding: 2px;
      gap: 2px;
    }
    .audio-switch button {
      background: transparent;
      color: #a1a1aa;
      padding: 0.42rem 0.75rem;
      border-radius: 6px;
      font-size: 0.84rem;
    }
    .audio-switch button.selected-mic { background: #16a34a; color: #fff; }
    .audio-switch button.selected-speaker { background: #2563eb; color: #fff; }

    .badge {
      background: #ef4444;
      color: #fff;
      font-size: 0.72rem;
      padding: 1px 6px;
      border-radius: 999px;
      margin-left: 3px;
    }
    .badge.amber { background: #d97706; }

    #call-body { order: 3; flex: 1; display: flex; min-height: 0; position: relative; }
    #stage { flex: 1; display: flex; flex-direction: column; position: relative; min-width: 0; overflow: hidden; }

    /* Mobile Swipe View Bar & Toast */
    #mobile-view-bar {
      display: none;
      align-items: center;
      justify-content: center;
      gap: 0.35rem;
      padding: 0.35rem 0.5rem;
      background: #12151b;
      border-bottom: 1px solid #242934;
      flex-shrink: 0;
      overflow-x: auto;
    }
    .m-view-pill {
      padding: 0.24rem 0.55rem;
      font-size: 0.72rem;
      border-radius: 999px;
      background: #232834;
      color: #a1a1aa;
      white-space: nowrap;
    }
    .m-view-pill.active-pill { background: #2563eb; color: #fff; }
    .swipe-hint { font-size: 0.68rem; color: #71717a; margin-left: 4px; white-space: nowrap; }

    #view-swipe-toast {
      position: absolute;
      top: 52px;
      left: 50%;
      transform: translateX(-50%);
      background: rgba(17, 24, 39, 0.92);
      border: 1px solid #3b82f6;
      color: #fff;
      padding: 0.42rem 0.9rem;
      border-radius: 999px;
      font-size: 0.84rem;
      font-weight: 600;
      z-index: 40;
      pointer-events: none;
      box-shadow: 0 8px 20px rgba(0, 0, 0, 0.45);
      animation: popIn 0.16s ease-out;
    }

    @keyframes swipeInNext {
      from { opacity: 0.55; transform: translateX(30px); }
      to { opacity: 1; transform: translateX(0); }
    }
    @keyframes swipeInPrev {
      from { opacity: 0.55; transform: translateX(-30px); }
      to { opacity: 1; transform: translateX(0); }
    }
    #video-grid.swipe-next { animation: swipeInNext 0.22s ease-out; }
    #video-grid.swipe-prev { animation: swipeInPrev 0.22s ease-out; }

    /* Join Options & Waiting Room Overlays */
    .stage-overlay {
      position: absolute;
      inset: 0;
      background: rgba(10, 12, 16, 0.88);
      backdrop-filter: blur(6px);
      z-index: 30;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 1rem;
    }
    .join-modal {
      background: #181b22;
      border: 1px solid #333844;
      border-radius: 14px;
      padding: 1.5rem;
      width: min(94%, 440px);
      max-height: 88dvh;
      overflow-y: auto;
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 1rem;
      text-align: center;
      box-shadow: 0 20px 50px rgba(0, 0, 0, 0.6);
    }
    .join-modal h2 { margin: 0; font-size: 1.25rem; }
    .join-modal p { margin: 0; font-size: 0.88rem; color: #a1a1aa; }
    .join-choice-grid { display: grid; gap: 0.65rem; width: 100%; }
    .join-choice-btn {
      padding: 0.85rem 1rem;
      font-size: 0.95rem;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 0.5rem;
    }
    .spinner {
      width: 28px;
      height: 28px;
      border: 3px solid #333844;
      border-top-color: #3b82f6;
      border-radius: 50%;
      animation: spin 0.9s linear infinite;
    }
    @keyframes spin { to { transform: rotate(360deg); } }

    /* Banners */
    #banner-container {
      position: absolute;
      top: 12px;
      left: 50%;
      transform: translateX(-50%);
      z-index: 45;
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
      width: min(92%, 520px);
      pointer-events: none;
    }
    .banner-card {
      pointer-events: auto;
      background: #1f2430;
      border: 1px solid #3b82f6;
      border-radius: 10px;
      padding: 0.75rem 1rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 0.65rem;
      box-shadow: 0 10px 25px rgba(0, 0, 0, 0.55);
      animation: popIn 0.2s ease-out;
    }
    .banner-card.approved { border-color: #16a34a; background: #132a1c; }
    .banner-card.denied { border-color: #dc2626; background: #3b1212; }
    .banner-card.waiting-req { border-color: #f59e0b; background: #261e12; }
    .banner-text { font-size: 0.88rem; line-height: 1.4; display: flex; align-items: center; gap: 0.55rem; }

    /* Dedicated HD Screen Share Stage */
    #screen-stage {
      background: #090b0f;
      border-bottom: 1px solid #2b303b;
      padding: 0.6rem 1rem;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      max-height: 55dvh;
      position: relative;
      flex-shrink: 0;
    }
    .screen-stage-header {
      width: 100%;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 0.75rem;
      margin-bottom: 0.4rem;
      flex-shrink: 0;
    }
    #screen-stage-label { font-size: 0.84rem; color: #93c5fd; font-weight: 600; }
    #screen-fullscreen-btn { padding: 0.3rem 0.7rem; font-size: 0.8rem; flex-shrink: 0; }
    #screen-stage video {
      max-height: 45dvh;
      max-width: 100%;
      border-radius: 8px;
      background: #000;
      cursor: pointer;
    }
    #screen-stage:fullscreen,
    #screen-stage:-webkit-full-screen {
      max-height: 100vh;
      width: 100vw;
      height: 100vh;
      padding: 0.45rem 0.75rem;
      background: #000;
      justify-content: flex-start;
    }
    #screen-stage:fullscreen .screen-stage-header,
    #screen-stage:-webkit-full-screen .screen-stage-header { margin-bottom: 0.35rem; }
    #screen-stage:fullscreen video,
    #screen-stage:-webkit-full-screen video {
      flex: 1;
      min-height: 0;
      max-height: none;
      width: 100%;
      height: 100%;
      object-fit: contain;
      border-radius: 0;
    }

    /* Collaborative Whiteboard Stage */
    #whiteboard-stage {
      background: #14171d;
      border-bottom: 1px solid #2b303b;
      padding: 0.55rem 0.85rem;
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 0.45rem;
      max-height: 64dvh;
      flex-shrink: 0;
      position: relative;
    }
    .wb-toolbar {
      width: 100%;
      display: flex;
      align-items: center;
      gap: 0.4rem;
      overflow-x: auto;
      -webkit-overflow-scrolling: touch;
      padding-bottom: 2px;
      flex-shrink: 0;
    }
    .wb-toolbar button {
      padding: 0.34rem 0.6rem;
      font-size: 0.78rem;
      white-space: nowrap;
      flex-shrink: 0;
    }
    .wb-toolbar button.wb-tool-active { background: #2563eb; outline: 2px solid #93c5fd; }
    .wb-colors {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      background: #101217;
      padding: 3px 6px;
      border-radius: 8px;
      border: 1px solid #333844;
      flex-shrink: 0;
    }
    .wb-swatch {
      width: 20px;
      height: 20px;
      min-width: 20px;
      padding: 0;
      border-radius: 50%;
      border: 2px solid transparent;
    }
    .wb-swatch.active-swatch { border-color: #fff; transform: scale(1.15); }
    .wb-canvas-wrap {
      position: relative;
      width: 100%;
      max-width: 960px;
      aspect-ratio: 16 / 9;
      max-height: 48dvh;
      background: #ffffff;
      border-radius: 8px;
      overflow: hidden;
      border: 1px solid #333844;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    .wb-canvas-wrap canvas {
      position: absolute;
      inset: 0;
      width: 100%;
      height: 100%;
      touch-action: none;
      cursor: crosshair;
    }
    #wb-preview-canvas { pointer-events: none; }
    .wb-lock-pill {
      font-size: 0.76rem;
      color: #fde68a;
      background: #78350f;
      padding: 3px 8px;
      border-radius: 6px;
      white-space: nowrap;
      flex-shrink: 0;
    }
    #whiteboard-stage:fullscreen,
    #whiteboard-stage:-webkit-full-screen {
      max-height: 100vh;
      width: 100vw;
      height: 100vh;
      padding: 0.65rem;
      background: #0f1115;
    }
    #whiteboard-stage:fullscreen .wb-canvas-wrap,
    #whiteboard-stage:-webkit-full-screen .wb-canvas-wrap {
      max-width: none;
      max-height: calc(100vh - 70px);
      height: calc(100vh - 70px);
      width: auto;
    }

    /* --- Meeting View Layouts --- */
    #video-grid {
      flex: 1;
      padding: 0.85rem;
      overflow-y: auto;
      -webkit-overflow-scrolling: touch;
      touch-action: pan-y;
    }
    #video-grid.mode-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
      gap: 0.75rem;
      align-content: start;
      justify-items: center;
    }
    #video-grid.mode-compact {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(155px, 1fr));
      gap: 0.5rem;
      align-content: start;
      justify-items: center;
    }
    #video-grid.mode-compact .avatar-circle.large { width: 46px; height: 46px; font-size: 1rem; }
    #video-grid.mode-compact .ph-text { font-size: 0.72rem; }
    #video-grid.mode-compact .label { font-size: 0.7rem; padding: 2px 6px; bottom: 5px; left: 5px; }

    #video-grid.mode-speaker {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(175px, 1fr));
      grid-auto-rows: max-content;
      gap: 0.65rem;
      align-content: start;
    }
    #video-grid.mode-speaker .video-tile { max-width: none; }
    #video-grid.mode-speaker .video-tile.featured-tile {
      order: -1;
      grid-column: 1 / -1;
      max-height: 55dvh;
      width: 100%;
      max-width: 1040px;
      justify-self: center;
    }

    #video-grid.mode-sidebar {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 230px;
      grid-auto-rows: min-content;
      gap: 0.75rem;
      align-content: start;
    }
    #video-grid.mode-sidebar.single-tile { grid-template-columns: 1fr; }
    #video-grid.mode-sidebar .video-tile { grid-column: 2; max-width: none; }
    #video-grid.mode-sidebar .video-tile.featured-tile {
      grid-column: 1;
      grid-row: 1 / span 24;
      position: sticky;
      top: 0;
      max-width: none;
      max-height: calc(100dvh - 210px);
      height: 100%;
      align-self: stretch;
    }

    .video-tile {
      position: relative;
      width: 100%;
      max-width: 640px;
      aspect-ratio: 16 / 9;
      background: #181b22;
      border-radius: 10px;
      overflow: hidden;
      border: 2px solid #2b303b;
      display: flex;
      align-items: center;
      justify-content: center;
      transition: border-color 0.15s ease, box-shadow 0.15s ease;
    }

    /* Active Speaker Light-Up */
    .video-tile.speaking {
      border-color: #22c55e;
      box-shadow: 0 0 0 2px rgba(34, 197, 94, 0.55), 0 0 24px rgba(34, 197, 94, 0.45);
    }
    .video-tile.speaking .placeholder {
      background: radial-gradient(circle at center, #1b3625 0%, #181b22 72%);
    }
    .video-tile.speaking .avatar-circle {
      border-color: #4ade80;
      box-shadow: 0 0 22px rgba(74, 222, 128, 0.9);
      transform: scale(1.07);
    }
    .video-tile.speaking .label {
      background: rgba(22, 163, 74, 0.92);
      color: #fff;
      font-weight: 700;
    }

    .video-tile video {
      position: relative;
      z-index: 0;
      width: 100%;
      height: 100%;
      object-fit: cover;
      background: #000;
    }
    .placeholder {
      position: absolute;
      inset: 0;
      z-index: 1;
      display: flex;
      flex-direction: column;
      gap: 0.45rem;
      align-items: center;
      justify-content: center;
      color: #a1a1aa;
      font-size: 0.85rem;
      background: #181b22;
      text-align: center;
      padding: 0.65rem;
      transition: background 0.15s ease;
    }
    .label {
      position: absolute;
      bottom: 8px;
      left: 8px;
      background: rgba(0, 0, 0, 0.72);
      padding: 3px 8px;
      border-radius: 6px;
      font-size: 0.78rem;
      z-index: 2;
      max-width: calc(100% - 16px);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      transition: background 0.15s ease;
    }
    .pin-btn {
      position: absolute;
      top: 8px;
      left: 8px;
      z-index: 3;
      background: rgba(0, 0, 0, 0.65);
      color: #e4e4e7;
      padding: 3px 7px;
      border-radius: 6px;
      font-size: 0.75rem;
      opacity: 0;
      transition: opacity 0.15s, background 0.15s;
    }
    .video-tile:hover .pin-btn, .pin-btn.pinned { opacity: 1; }
    .pin-btn.pinned { background: #2563eb; color: #fff; }
    .tile-reaction {
      position: absolute;
      top: 8px;
      right: 8px;
      background: rgba(0, 0, 0, 0.75);
      border: 1px solid rgba(255, 255, 255, 0.15);
      padding: 5px 9px;
      border-radius: 999px;
      font-size: 1.3rem;
      z-index: 3;
      animation: popIn 0.2s ease-out;
    }

    /* Live Auto Captions Bar */
    #caption-bar {
      position: absolute;
      bottom: 58px;
      left: 50%;
      transform: translateX(-50%);
      max-width: min(92%, 760px);
      background: rgba(0, 0, 0, 0.84);
      border: 1px solid rgba(255, 255, 255, 0.16);
      color: #fff;
      padding: 0.5rem 0.9rem;
      border-radius: 10px;
      font-size: 0.95rem;
      line-height: 1.4;
      text-align: center;
      z-index: 15;
      pointer-events: none;
      box-shadow: 0 6px 20px rgba(0, 0, 0, 0.5);
    }
    #caption-speaker { color: #93c5fd; font-weight: 700; margin-right: 6px; }

    /* Footer Controls */
    #stage-footer {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 0.5rem;
      padding: 0.45rem 0.85rem;
      background: #14171d;
      border-top: 1px solid #2b303b;
      flex-shrink: 0;
      flex-wrap: wrap;
    }
    #pagination-bar {
      display: flex;
      align-items: center;
      gap: 0.45rem;
      font-size: 0.82rem;
      color: #d4d4d8;
    }
    #pagination-bar button { padding: 0.35rem 0.65rem; font-size: 0.78rem; }
    #reaction-bar {
      display: flex;
      justify-content: center;
      gap: 0.35rem;
      flex-wrap: wrap;
      margin: 0 auto;
    }
    .react-btn {
      background: #232834;
      border: 1px solid #323949;
      border-radius: 999px;
      padding: 0.35rem 0.68rem;
      font-size: 1.1rem;
      line-height: 1;
      transition: transform 0.12s, background 0.12s;
    }
    .react-btn:hover { transform: scale(1.15); background: #303746; }

    #reaction-layer {
      position: absolute;
      inset: 0;
      pointer-events: none;
      overflow: hidden;
      z-index: 10;
    }
    .floating-reaction {
      position: absolute;
      bottom: 60px;
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 2px;
      animation: floatUp 2.6s ease-out forwards;
    }
    .floating-reaction .emoji { font-size: 2.2rem; }
    .floating-reaction .who {
      font-size: 0.72rem;
      background: rgba(0, 0, 0, 0.75);
      padding: 2px 7px;
      border-radius: 999px;
      color: #e4e4e7;
    }

    @keyframes floatUp {
      0% { transform: translateY(0) scale(0.7); opacity: 0; }
      15% { transform: translateY(-20px) scale(1.1); opacity: 1; }
      100% { transform: translateY(-240px) scale(1); opacity: 0; }
    }
    @keyframes popIn {
      0% { transform: scale(0.85); opacity: 0; }
      100% { transform: scale(1); opacity: 1; }
    }

    /* Sidebars (Chat & Participants) */
    .side-panel {
      width: 340px;
      background: #181b22;
      border-left: 1px solid #2b303b;
      display: flex;
      flex-direction: column;
      flex-shrink: 0;
      height: 100%;
    }
    .panel-header {
      padding: 0.75rem 0.9rem;
      border-bottom: 1px solid #2b303b;
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-weight: 700;
      font-size: 0.95rem;
    }
    .panel-scroll {
      flex: 1;
      overflow-y: auto;
      padding: 0.75rem;
      display: flex;
      flex-direction: column;
      gap: 0.6rem;
    }
    .p-section-title {
      font-size: 0.75rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: #a1a1aa;
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-top: 0.25rem;
    }
    .participant-row {
      background: #101217;
      border: 1px solid #2b303b;
      border-radius: 8px;
      padding: 0.5rem 0.65rem;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 0.55rem;
      transition: border-color 0.15s ease, background 0.15s ease;
    }
    .participant-row.speaking { border-color: #22c55e; background: #13281b; }
    .participant-row.speaking .avatar-circle {
      border-color: #4ade80;
      box-shadow: 0 0 12px rgba(74, 222, 128, 0.85);
    }
    .participant-left { display: flex; align-items: center; gap: 0.55rem; min-width: 0; flex: 1; }
    .participant-name-wrap { display: flex; flex-direction: column; min-width: 0; }
    .participant-name { font-size: 0.88rem; font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .participant-tags { display: flex; gap: 4px; flex-wrap: wrap; margin-top: 2px; }
    .mini-tag { font-size: 0.65rem; padding: 1px 5px; border-radius: 4px; background: #272c38; color: #d4d4d8; }
    .mini-tag.host { background: #7c3aed; color: #fff; }
    .mini-tag.screen { background: #16a34a; color: #fff; }
    .participant-right { display: flex; align-items: center; gap: 0.35rem; flex-shrink: 0; font-size: 0.88rem; }
    .waiting-banner-box {
      background: #261e12;
      border: 1px solid #d97706;
      border-radius: 8px;
      padding: 0.65rem;
      font-size: 0.84rem;
      color: #fde68a;
    }

    .chat-tabs-bar { display: flex; align-items: center; border-bottom: 1px solid #2b303b; }
    .chat-tabs { display: flex; flex: 1; }
    .chat-tab {
      flex: 1;
      background: transparent;
      border-radius: 0;
      padding: 0.75rem 0.5rem;
      color: #a1a1aa;
      border-bottom: 2px solid transparent;
      font-size: 0.88rem;
    }
    .chat-tab.active-tab { color: #fff; border-bottom-color: #2563eb; background: #1e222b; }
    .dm-bar { padding: 0.65rem 0.75rem; border-bottom: 1px solid #2b303b; background: #14171d; }
    .chat-messages {
      flex: 1;
      overflow-y: auto;
      padding: 0.85rem;
      display: flex;
      flex-direction: column;
      gap: 0.65rem;
    }
    .msg-item { display: flex; flex-direction: column; max-width: 88%; align-self: flex-start; }
    .msg-item.self { align-self: flex-end; }
    .msg-meta { font-size: 0.72rem; color: #a1a1aa; margin-bottom: 3px; }
    .msg-item.self .msg-meta { text-align: right; }
    .msg-bubble {
      background: #272c38;
      color: #f4f4f5;
      padding: 0.5rem 0.75rem;
      border-radius: 10px;
      font-size: 0.88rem;
      line-height: 1.35;
      word-break: break-word;
    }
    .msg-item.self .msg-bubble { background: #2563eb; color: #fff; }
    .msg-item.dm .msg-bubble { border: 1px solid #9333ea; }
    .msg-item.dm:not(.self) .msg-bubble { background: #2e1f47; cursor: pointer; }
    .dm-tag {
      display: inline-block;
      font-size: 0.68rem;
      background: #7e22ce;
      color: #fff;
      padding: 1px 5px;
      border-radius: 4px;
      margin-right: 4px;
    }
    .chat-input-row {
      padding: 0.7rem;
      border-top: 1px solid #2b303b;
      display: flex;
      gap: 0.5rem;
      background: #14171d;
    }
    .chat-input-row input { flex: 1; padding: 0.5rem 0.7rem; font-size: 0.9rem; }

    /* ==================== MOBILE RESPONSIVE ==================== */
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

      #controls-dock {
        order: 4;
        border-bottom: none;
        border-top: 1px solid #2b303b;
        padding: 0.45rem 0.55rem calc(0.45rem + env(safe-area-inset-bottom));
        justify-content: center;
      }
      .primary-controls {
        width: 100%;
        justify-content: space-between;
        gap: 0.35rem;
        flex-wrap: nowrap;
      }
      .primary-controls .audio-switch { flex: 1.4; justify-content: center; }
      .primary-controls .audio-switch button { flex: 1; padding: 0.5rem 0.35rem; font-size: 0.78rem; }
      .primary-controls > button { flex: 1; padding: 0.52rem 0.4rem; font-size: 0.78rem; }

      .secondary-controls {
        display: none;
        position: absolute;
        bottom: 100%;
        left: 0;
        right: 0;
        background: #1c2029;
        border-top: 1px solid #333844;
        padding: 0.75rem;
        gap: 0.5rem;
        flex-wrap: wrap;
        justify-content: center;
        box-shadow: 0 -10px 30px rgba(0, 0, 0, 0.55);
      }
      #controls-dock.more-open .secondary-controls { display: flex; }
      .secondary-controls button,
      .secondary-controls select {
        flex: 1 1 44%;
        font-size: 0.84rem;
        padding: 0.55rem 0.65rem;
      }

      #video-grid { padding: 0.5rem; }
      #video-grid.mode-grid {
        grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
        gap: 0.5rem;
      }
      #video-grid.mode-compact {
        grid-template-columns: repeat(auto-fill, minmax(105px, 1fr));
        gap: 0.4rem;
      }
      #video-grid.mode-speaker {
        grid-template-columns: repeat(auto-fill, minmax(130px, 1fr));
        gap: 0.45rem;
      }
      #video-grid.mode-speaker .video-tile.featured-tile { max-height: 36dvh; }

      #video-grid.mode-sidebar {
        grid-template-columns: minmax(0, 1fr) 110px;
        gap: 0.45rem;
      }
      #video-grid.mode-sidebar .video-tile { grid-column: 2; }
      #video-grid.mode-sidebar .video-tile.featured-tile {
        grid-column: 1;
        grid-row: 1 / span 24;
        position: sticky;
        top: 0;
        max-height: 44dvh;
      }

      .pin-btn { opacity: 0.85; padding: 2px 6px; font-size: 0.7rem; }
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

      .side-panel {
        position: absolute;
        inset: 0;
        width: 100%;
        max-width: 100%;
        z-index: 35;
      }
    }
  </style>
</head>
<body>

  <!-- LOBBY & SCHEDULER -->
  <div id="lobby-view">
    <div class="lobby-header">
      <h1>Fire-hub Video Conferencing</h1>

      <div class="profile-setup">
        <div id="profile-avatar-preview" class="avatar-circle">U</div>
        <div style="flex: 1; min-width: 150px;">
          <label for="name-input" style="margin-bottom: 2px;">Your Display Name</label>
          <input id="name-input" type="text" placeholder="Enter your name" style="padding: 0.45rem 0.65rem;" />
        </div>
        <div class="row">
          <input id="avatar-file-input" type="file" accept="image/*" class="hidden" />
          <button id="upload-avatar-btn" class="secondary" type="button" style="padding: 0.45rem 0.75rem;">
            🖼️ Photo
          </button>
          <button id="clear-avatar-btn" class="danger hidden" type="button" style="padding: 0.45rem 0.65rem;" title="Remove Photo">
            ✕
          </button>
        </div>
      </div>
    </div>

    <!-- Invitation Link View -->
    <div id="direct-room-card" class="card hidden">
      <h2 id="direct-room-title">Ready to join meeting</h2>
      <div id="direct-room-time" class="meeting-sub"></div>
      <div id="direct-room-link" class="link-box"></div>
      <div class="row">
        <button id="direct-join-btn">Join Meeting Now</button>
        <button id="direct-copy-btn" class="secondary">Copy Link</button>
      </div>
    </div>

    <!-- Home Dashboard Sections -->
    <div id="lobby-home-sections">
      <div class="cards">
        <div class="card">
          <h2>Start or Join a Meeting</h2>
          <div>
            <label for="instant-join-mode">Room Joining Permission</label>
            <select id="instant-join-mode">
              <option value="direct">Direct Join (No Owner Approval)</option>
              <option value="approval">Need Owner's Approval (Waiting Room)</option>
            </select>
          </div>
          <label class="checkbox-row">
            <input id="instant-approval-chk" type="checkbox" checked />
            Require host approval to share screen
          </label>
          <label class="checkbox-row">
            <input id="instant-record-chk" type="checkbox" />
            Record this meeting automatically
          </label>
          <button id="instant-btn" class="active">New Instant Meeting</button>
          <hr style="border: 0; border-top: 1px solid #2b303b; width: 100%; margin: 0.25rem 0;" />
          <div>
            <label for="room-input">Join with Meeting Code or Link</label>
            <div class="row">
              <input id="room-input" type="text" placeholder="e.g. abc-defg-hij" style="flex: 1;" />
              <button id="join-btn">Join</button>
            </div>
          </div>
        </div>

        <div class="card">
          <h2>Schedule a Meeting</h2>
          <div>
            <label for="sched-title">Meeting Title</label>
            <input id="sched-title" type="text" placeholder="e.g. All-Hands (100 Users)" />
          </div>
          <div>
            <label for="sched-time">Date & Time</label>
            <input id="sched-time" type="datetime-local" />
          </div>
          <div>
            <label for="sched-join-mode">Room Joining Permission</label>
            <select id="sched-join-mode">
              <option value="direct">Direct Join (No Owner Approval)</option>
              <option value="approval">Need Owner's Approval (Waiting Room)</option>
            </select>
          </div>
          <label class="checkbox-row">
            <input id="sched-approval-chk" type="checkbox" checked />
            Require host approval to share screen
          </label>
          <label class="checkbox-row">
            <input id="sched-record-chk" type="checkbox" />
            Record this meeting automatically
          </label>
          <button id="sched-btn">Create Scheduled Link</button>
          <div id="sched-result" class="hidden">
            <div class="link-box" id="sched-link-box"></div>
          </div>
        </div>
      </div>

      <!-- ONLY visible to owner on main host server -->
      <div id="scheduled-meetings-card" class="card hidden">
        <div class="row" style="justify-content: space-between;">
          <h2>Scheduled Meetings</h2>
          <button id="refresh-list-btn" class="secondary" style="padding: 0.35rem 0.75rem;">Refresh</button>
        </div>
        <div id="meeting-list" class="meeting-list">
          <div class="meeting-sub">Loading meetings...</div>
        </div>
      </div>

      <!-- ONLY visible to owner on main host server -->
      <div id="saved-recordings-card" class="card hidden">
        <div class="row" style="justify-content: space-between;">
          <h2>Saved Recordings (D:\video-app\recordings)</h2>
          <button id="refresh-rec-btn" class="secondary" style="padding: 0.35rem 0.75rem;">Refresh</button>
        </div>
        <div id="recording-list" class="meeting-list">
          <div class="meeting-sub">Loading recordings...</div>
        </div>
      </div>
    </div>
  </div>

  <!-- IN-APP RECORDING PLAYER MODAL -->
  <div id="player-modal" class="hidden">
    <div class="player-box">
      <div class="player-header">
        <strong id="player-title">Recording</strong>
        <button id="player-close-btn" class="danger" style="padding: 0.35rem 0.75rem;">✕ Close</button>
      </div>
      <video id="player-video" controls autoplay playsinline></video>
    </div>
  </div>

  <!-- ACTIVE CALL VIEW -->
  <div id="call-view" class="hidden">
    <header id="call-header">
      <div class="call-info">
        <span class="call-title">
          <span id="call-title-text">Meeting</span>
          <span id="host-badge" class="host-pill hidden">Host</span>
          <span id="participant-count" class="count-pill">👥 1 / 100</span>
          <span id="joining-count-pill" class="joining-pill hidden">⏳ 0</span>
          <span id="recording-pill" class="rec-pill hidden">● REC</span>
        </span>
        <span class="call-sub" id="call-sub"></span>
      </div>

      <div class="header-actions">
        <button id="copy-call-link-btn" class="secondary">🔗 <span class="hide-sm">Copy Link</span></button>
        <button id="leave-btn" class="danger">Leave</button>
      </div>
    </header>

    <!-- Controls Dock -->
    <div id="controls-dock">
      <div class="primary-controls">
        <div class="audio-switch" title="Switch audio mode">
          <button id="use-mic-btn" type="button">🎤 <span class="hide-sm">With </span>Mic</button>
          <button id="use-speaker-btn" type="button">🔊 Speaker<span class="hide-sm"> Only</span></button>
        </div>

        <button id="cam-btn" class="secondary">📷 <span class="hide-sm">Enable </span>Cam</button>

        <button id="toggle-participants-btn" class="secondary">
          👥 <span class="hide-sm">Participants </span>(<span id="participants-btn-count">1</span>)
          <span id="joining-btn-badge" class="badge amber hidden">0</span>
        </button>

        <button id="toggle-chat-btn" class="secondary">
          💬 <span class="hide-sm">Chat</span><span id="chat-total-badge" class="badge hidden">0</span>
        </button>

        <button id="mobile-more-btn" class="secondary mobile-only" type="button">⋯ More</button>
      </div>

      <div class="secondary-controls">
        <select id="view-mode-select" title="Switch Meeting View">
          <option value="grid">📐 Grid View</option>
          <option value="speaker">🎤 Speaker View</option>
          <option value="sidebar">◫ Sidebar View</option>
          <option value="compact">🪪 Compact View</option>
        </select>

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
        <div id="banner-container"></div>
        <div id="view-swipe-toast" class="hidden"></div>

        <!-- On-Screen Join Audio & Camera Choice -->
        <div id="join-options-overlay" class="stage-overlay hidden">
          <div class="join-modal">
            <div id="modal-avatar-preview" class="avatar-circle large">U</div>
            <h2 id="join-modal-title">Choose Audio & Camera</h2>
            <p>Select how you want to join this meeting. You can switch anytime on screen.</p>

            <div class="join-choice-grid">
              <button id="modal-mic-btn" class="active join-choice-btn">
                🎤 Join with Microphone
              </button>
              <button id="modal-speaker-btn" class="join-choice-btn">
                🔊 Join with Only Computer Speaker
              </button>
            </div>

            <hr style="border: 0; border-top: 1px solid #2b303b; width: 100%; margin: 0.2rem 0;" />

            <div class="row" style="justify-content: center; width: 100%;">
              <button id="modal-cam-btn" class="secondary" style="width: 100%;">
                📷 Enable Camera
              </button>
            </div>
          </div>
        </div>

        <!-- Waiting Room Overlay -->
        <div id="waiting-room-overlay" class="stage-overlay hidden">
          <div class="join-modal">
            <div id="waiting-avatar-preview" class="avatar-circle large">U</div>
            <div class="spinner"></div>
            <h2>Waiting Room</h2>
            <p id="waiting-room-status">Waiting for the meeting owner to let you in...</p>
            <button id="cancel-waiting-btn" class="danger" style="width: 100%;">Leave Waiting Room</button>
          </div>
        </div>

        <div id="reaction-layer"></div>
        <div id="audio-container" class="hidden"></div>

        <!-- Mobile Swipeable View Pills -->
        <div id="mobile-view-bar">
          <button type="button" class="m-view-pill" data-mode="grid">📐 Grid</button>
          <button type="button" class="m-view-pill" data-mode="speaker">🎤 Speaker</button>
          <button type="button" class="m-view-pill" data-mode="sidebar">◫ Sidebar</button>
          <button type="button" class="m-view-pill" data-mode="compact">🪪 Compact</button>
          <span class="swipe-hint">⇄ Swipe</span>
        </div>

        <!-- Shared Screen Stage -->
        <div id="screen-stage" class="hidden">
          <div class="screen-stage-header">
            <div id="screen-stage-label">Screen Share</div>
            <button id="screen-fullscreen-btn" class="secondary hidden" type="button">⛶ Fullscreen</button>
          </div>
          <video id="screen-stage-video" autoplay playsinline title="Double-click for fullscreen"></video>
        </div>

        <!-- Collaborative Whiteboard Stage -->
        <div id="whiteboard-stage" class="hidden">
          <div class="wb-toolbar">
            <button class="secondary wb-tool wb-tool-active" data-tool="pen">✏️ Pen</button>
            <button class="secondary wb-tool" data-tool="highlighter">🖍️ Marker</button>
            <button class="secondary wb-tool" data-tool="line">📏 Line</button>
            <button class="secondary wb-tool" data-tool="rect">⬜ Rect</button>
            <button class="secondary wb-tool" data-tool="circle">⭕ Circle</button>
            <button class="secondary wb-tool" data-tool="text">🔤 Text</button>
            <button class="secondary wb-tool" data-tool="eraser">🧹 Eraser</button>

            <div class="wb-colors">
              <button class="wb-swatch active-swatch" data-color="#111827" style="background:#111827;" title="Black"></button>
              <button class="wb-swatch" data-color="#dc2626" style="background:#dc2626;" title="Red"></button>
              <button class="wb-swatch" data-color="#2563eb" style="background:#2563eb;" title="Blue"></button>
              <button class="wb-swatch" data-color="#16a34a" style="background:#16a34a;" title="Green"></button>
              <button class="wb-swatch" data-color="#d97706" style="background:#d97706;" title="Orange"></button>
              <button class="wb-swatch" data-color="#9333ea" style="background:#9333ea;" title="Purple"></button>
            </div>

            <select id="wb-size" style="width:auto; padding:0.3rem 0.5rem; font-size:0.78rem;">
              <option value="3">Thin</option>
              <option value="6" selected>Medium</option>
              <option value="12">Thick</option>
            </select>

            <button id="wb-undo-btn" class="secondary" type="button">↩️ Undo</button>
            <button id="wb-clear-btn" class="danger" type="button">🗑️ Clear</button>
            <button id="wb-save-btn" class="secondary" type="button">💾 Save</button>
            <button id="wb-lock-btn" class="secondary hidden" type="button">🔓 Everyone Draws</button>
            <span id="wb-locked-pill" class="wb-lock-pill hidden">🔒 Host Only</span>
            <button id="wb-fullscreen-btn" class="secondary" type="button">⛶ Fullscreen</button>
            <button id="wb-close-btn" class="danger" type="button">✕ Close</button>
          </div>

          <div class="wb-canvas-wrap">
            <canvas id="wb-canvas" width="1600" height="900"></canvas>
            <canvas id="wb-preview-canvas" width="1600" height="900"></canvas>
          </div>
        </div>

        <main id="video-grid" class="mode-grid"></main>

        <div id="caption-bar" class="hidden">
          <span id="caption-speaker"></span>
          <span id="caption-text">Auto Captions Enabled (listening...)</span>
        </div>

        <div id="stage-footer">
          <div id="pagination-bar" class="hidden">
            <button id="prev-page-btn" class="secondary">◀ Prev</button>
            <span id="page-indicator">Page 1 / 1</span>
            <button id="next-page-btn" class="secondary">Next ▶</button>
          </div>

          <div id="reaction-bar">
            <button class="react-btn" data-emoji="👍" title="Thumbs Up">👍</button>
            <button class="react-btn" data-emoji="❤️" title="Love">❤️</button>
            <button class="react-btn" data-emoji="😂" title="Laugh">😂</button>
            <button class="react-btn" data-emoji="👏" title="Clap">👏</button>
            <button class="react-btn" data-emoji="🎉" title="Celebrate">🎉</button>
            <button class="react-btn" data-emoji="😮" title="Wow">😮</button>
          </div>
        </div>
      </div>

      <!-- PARTICIPANTS SIDEBAR -->
      <aside id="participants-panel" class="side-panel hidden">
        <div class="panel-header">
          <span id="participants-panel-title">Participants (1)</span>
          <button id="close-participants-btn" class="secondary" style="padding: 0.25rem 0.55rem;">✕</button>
        </div>
        <div style="padding: 0.65rem 0.75rem; border-bottom: 1px solid #2b303b;">
          <input id="participant-search" type="text" placeholder="Search participants..." style="padding: 0.45rem 0.65rem;" />
        </div>
        <div id="participants-list" class="panel-scroll"></div>
      </aside>

      <!-- CHAT SIDEBAR -->
      <aside id="chat-panel" class="side-panel hidden">
        <div class="chat-tabs-bar">
          <div class="chat-tabs">
            <button id="tab-public" class="chat-tab active-tab">
              Public <span id="badge-public" class="badge hidden">0</span>
            </button>
            <button id="tab-private" class="chat-tab">
              Personal (DM) <span id="badge-private" class="badge hidden">0</span>
            </button>
          </div>
          <button id="close-chat-btn" class="secondary" style="margin: 0 0.5rem; padding: 0.25rem 0.55rem;">✕</button>
        </div>

        <div id="dm-bar" class="dm-bar hidden">
          <label for="dm-target" style="margin-bottom: 4px;">Send Private Message To:</label>
          <select id="dm-target"></select>
        </div>

        <div id="public-messages" class="chat-messages"></div>
        <div id="private-messages" class="chat-messages hidden"></div>

        <div class="chat-input-row">
          <input id="chat-input" type="text" placeholder="Message everyone..." />
          <button id="send-chat-btn">Send</button>
        </div>
      </aside>
    </div>
  </div>

  <script>
    const VIEW_MODES = ["grid", "speaker", "sidebar", "compact"];
    const VIEW_LABELS = {
      grid: "📐 Grid View",
      speaker: "🎤 Speaker View",
      sidebar: "◫ Sidebar View",
      compact: "🪪 Compact View"
    };

    // Lobby Elements
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

    // Player Modal
    const playerModal = document.getElementById("player-modal");
    const playerTitle = document.getElementById("player-title");
    const playerVideo = document.getElementById("player-video");
    const playerCloseBtn = document.getElementById("player-close-btn");

    // Call Elements
    const callTitleText = document.getElementById("call-title-text");
    const hostBadge = document.getElementById("host-badge");
    const participantCountEl = document.getElementById("participant-count");
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
    const viewSwipeToast = document.getElementById("view-swipe-toast");

    const useMicBtn = document.getElementById("use-mic-btn");
    const useSpeakerBtn = document.getElementById("use-speaker-btn");
    const camBtn = document.getElementById("cam-btn");
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

    // Whiteboard Elements
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

    // Overlay Elements
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

    // Participants Sidebar Elements
    const participantsPanel = document.getElementById("participants-panel");
    const participantsPanelTitle = document.getElementById("participants-panel-title");
    const closeParticipantsBtn = document.getElementById("close-participants-btn");
    const participantSearch = document.getElementById("participant-search");
    const participantsList = document.getElementById("participants-list");

    // Chat Elements
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

    // State
    const clientId = Math.random().toString(36).substring(2, 10);
    let isMainHostServer = false;
    let currentRoomId = null;
    let currentMeetingTitle = "Meeting";
    let maxUsersInRoom = 100;

    let ws = null;
    let lkRoom = null;
    let activeScreenPublication = null;
    let peerNames = {};
    let peerAvatars = {};
    let waitingUsers = {};
    let waitingCount = 0;
    let currentHostId = null;
    let tileOrder = ["local"];
    let currentPage = 1;
    let tileReactionTimers = {};

    let currentViewMode = localStorage.getItem("vc_view_mode") || "grid";
    let pinnedTileId = null;
    let lastActiveSpeakerTileId = null;
    let lkSpeakingIds = new Set();
    let localMicSpeaking = false;
    let localHoldTimer = null;
    let localVadCtx = null;
    let localVadTimer = null;
    let swipeToastTimer = null;

    let wantMicOnJoin = false;
    let wantCamOnJoin = false;
    let micEnabled = false;
    let camEnabled = false;
    let isSharingScreen = false;
    let hasJoinedRoom = false;

    let isHost = false;
    let requireScreenApproval = true;
    let requireJoinApproval = false;
    let autoRecordOnJoin = false;
    let isMeetingRecording = false;
    let activeSharer = null;
    let activeSharerName = null;
    let screenRequestPending = false;
    let screenShareApproved = false;

    // Whiteboard state
    let wbOpen = false;
    let wbLocked = false;
    let wbTool = "pen";
    let wbColor = "#111827";
    let wbHistory = [];
    let wbIsDrawing = false;
    let wbCurrentStroke = null;
    let wbLastPoint = null;
    let wbLastLiveSentAt = 0;

    // Recording state
    let isRecordingLocally = false;
    let mediaRecorder = null;
    let recordedChunks = [];
    let recAudioCtx = null;
    let recAudioDest = null;
    let recSilentOsc = null;
    let recConnectedTrackIds = new Set();
    let recCanvas = null;
    let recDrawTimer = null;
    let recWorker = null;
    let recWorkerUrl = null;
    let recStartTime = 0;
    let recStopPromise = null;
    let localScreenMirrorVideo = null;

    // Auto Captions state
    let captionsEnabled = false;
    let speechRecognition = null;
    let captionClearTimer = null;
    let lastCaptionSentAt = 0;

    let activeChatTab = "public";
    let unreadPublic = 0;
    let unreadPrivate = 0;

    let localAvatar = localStorage.getItem("vc_avatar") || "";

    if (window.innerWidth > 768) {
      chatPanel.classList.remove("hidden");
    }

    mobileMoreBtn.onclick = (e) => {
      e.stopPropagation();
      controlsDock.classList.toggle("more-open");
    };
    document.addEventListener("click", (e) => {
      if (!controlsDock.contains(e.target)) {
        controlsDock.classList.remove("more-open");
      }
    });

    // --- Check Main Host Server Access ---
    async function checkOwnerAccess() {
      try {
        const res = await fetch("/api/owner-status");
        const data = await res.json();
        isMainHostServer = Boolean(data.is_main_host);
      } catch {
        isMainHostServer = false;
      }

      scheduledMeetingsCard.classList.toggle("hidden", !isMainHostServer);
      savedRecordingsCard.classList.toggle("hidden", !isMainHostServer);

      if (isMainHostServer) {
        loadMeetings();
        loadRecordings();
      }
    }

    // --- Profile Name & Picture ---
    nameInput.value = localStorage.getItem("vc_display_name") || "User-" + clientId.slice(0, 4);
    nameInput.addEventListener("input", () => {
      localStorage.setItem("vc_display_name", nameInput.value.trim());
      renderAvatarPreviews();
    });

    function getDisplayName() { return nameInput.value.trim() || "Guest"; }

    function renderAvatarCircle(el, avatarUrl, name) {
      if (!el) return;
      if (avatarUrl) {
        el.innerHTML = `<img src="${escapeHtml(avatarUrl)}" alt="Avatar" />`;
      } else {
        el.textContent = (name || "U").trim().slice(0, 2).toUpperCase();
      }
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
      const file = e.target.files?.[0];
      if (!file) return;
      const reader = new FileReader();
      reader.onload = (ev) => {
        const img = new Image();
        img.onload = () => {
          const canvas = document.createElement("canvas");
          canvas.width = 128;
          canvas.height = 128;
          const ctx = canvas.getContext("2d");
          const size = Math.min(img.width, img.height);
          const sx = (img.width - size) / 2;
          const sy = (img.height - size) / 2;
          ctx.drawImage(img, sx, sy, size, size, 0, 0, 128, 128);
          localAvatar = canvas.toDataURL("image/jpeg", 0.78);
          try { localStorage.setItem("vc_avatar", localAvatar); } catch {}
          renderAvatarPreviews();
        };
        img.src = ev.target.result;
      };
      reader.readAsDataURL(file);
      avatarFileInput.value = "";
    };

    clearAvatarBtn.onclick = () => {
      localAvatar = "";
      localStorage.removeItem("vc_avatar");
      renderAvatarPreviews();
    };

    renderAvatarPreviews();

    // --- Collaborative Whiteboard Engine ---
    function canDrawOnWhiteboard() { return !wbLocked || isHost; }

    function updateWhiteboardUI() {
      whiteboardStage.classList.toggle("hidden", !wbOpen);
      wbToggleBtn.classList.toggle("active", wbOpen);
      wbToggleBtn.textContent = wbOpen ? "🎨 Close Board" : "🎨 Whiteboard";

      const writable = canDrawOnWhiteboard();
      wbCanvas.style.pointerEvents = writable ? "auto" : "none";
      wbCanvas.style.cursor = writable ? "crosshair" : "not-allowed";
      wbLockedPill.classList.toggle("hidden", !wbLocked || isHost);
      wbClearBtn.disabled = !writable;
      wbUndoBtn.disabled = !writable;

      wbLockBtn.classList.toggle("hidden", !isHost);
      if (isHost) {
        wbLockBtn.textContent = wbLocked ? "🔒 Host Only Draws" : "🔓 Everyone Draws";
        wbLockBtn.classList.toggle("warning", wbLocked);
        wbLockBtn.classList.toggle("secondary", !wbLocked);
      }
    }

    function drawSingleStroke(ctx, stroke) {
      const w = wbCanvas.width;
      const h = wbCanvas.height;
      ctx.save();
      ctx.lineCap = "round";
      ctx.lineJoin = "round";

      const tool = stroke.tool || "pen";
      const baseSize = Number(stroke.size || 6);

      if (tool === "eraser") {
        ctx.strokeStyle = "#ffffff";
        ctx.lineWidth = baseSize * 4;
      } else if (tool === "highlighter") {
        ctx.strokeStyle = stroke.color || "#111827";
        ctx.globalAlpha = 0.35;
        ctx.lineWidth = baseSize * 3.5;
      } else {
        ctx.strokeStyle = stroke.color || "#111827";
        ctx.fillStyle = stroke.color || "#111827";
        ctx.lineWidth = baseSize;
      }

      if (tool === "pen" || tool === "highlighter" || tool === "eraser") {
        const pts = stroke.points || [];
        if (pts.length === 1) {
          ctx.beginPath();
          ctx.arc(pts[0][0] * w, pts[0][1] * h, ctx.lineWidth / 2, 0, Math.PI * 2);
          ctx.fillStyle = tool === "eraser" ? "#ffffff" : ctx.strokeStyle;
          ctx.fill();
        } else if (pts.length > 1) {
          ctx.beginPath();
          ctx.moveTo(pts[0][0] * w, pts[0][1] * h);
          for (let i = 1; i < pts.length; i++) ctx.lineTo(pts[i][0] * w, pts[i][1] * h);
          ctx.stroke();
        }
      } else if (tool === "line") {
        ctx.beginPath();
        ctx.moveTo(stroke.x1 * w, stroke.y1 * h);
        ctx.lineTo(stroke.x2 * w, stroke.y2 * h);
        ctx.stroke();
      } else if (tool === "rect") {
        const x = Math.min(stroke.x1, stroke.x2) * w;
        const y = Math.min(stroke.y1, stroke.y2) * h;
        const rw = Math.abs(stroke.x2 - stroke.x1) * w;
        const rh = Math.abs(stroke.y2 - stroke.y1) * h;
        ctx.strokeRect(x, y, rw, rh);
      } else if (tool === "circle") {
        const cx = ((stroke.x1 + stroke.x2) / 2) * w;
        const cy = ((stroke.y1 + stroke.y2) / 2) * h;
        const rx = Math.abs(stroke.x2 - stroke.x1) * w / 2;
        const ry = Math.abs(stroke.y2 - stroke.y1) * h / 2;
        ctx.beginPath();
        ctx.ellipse(cx, cy, Math.max(1, rx), Math.max(1, ry), 0, 0, Math.PI * 2);
        ctx.stroke();
      } else if (tool === "text" && stroke.text) {
        const fontSize = Math.max(20, baseSize * 5);
        ctx.font = `600 ${fontSize}px system-ui, sans-serif`;
        ctx.textBaseline = "top";
        ctx.fillText(stroke.text, stroke.x * w, stroke.y * h);
      }
      ctx.restore();
    }

    function redrawWhiteboard() {
      wbCtx.fillStyle = "#ffffff";
      wbCtx.fillRect(0, 0, wbCanvas.width, wbCanvas.height);
      for (const stroke of wbHistory) drawSingleStroke(wbCtx, stroke);
    }

    function getNormalizedPoint(e) {
      const rect = wbCanvas.getBoundingClientRect();
      return [
        Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width)),
        Math.max(0, Math.min(1, (e.clientY - rect.top) / rect.height))
      ];
    }

    document.querySelectorAll(".wb-tool").forEach(btn => {
      btn.onclick = () => {
        wbTool = btn.dataset.tool;
        document.querySelectorAll(".wb-tool").forEach(b => b.classList.remove("wb-tool-active"));
        btn.classList.add("wb-tool-active");
      };
    });

    document.querySelectorAll(".wb-swatch").forEach(btn => {
      btn.onclick = () => {
        wbColor = btn.dataset.color;
        document.querySelectorAll(".wb-swatch").forEach(b => b.classList.remove("active-swatch"));
        btn.classList.add("active-swatch");
        if (wbTool === "eraser") document.querySelector('.wb-tool[data-tool="pen"]')?.click();
      };
    });

    wbCanvas.addEventListener("pointerdown", (e) => {
      if (!canDrawOnWhiteboard()) return;
      e.preventDefault();
      const [nx, ny] = getNormalizedPoint(e);
      const size = Number(wbSizeSelect.value || 6);

      if (wbTool === "text") {
        const text = prompt("Enter text for whiteboard:");
        if (text && text.trim()) {
          const stroke = {
            id: `${clientId}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
            sender: clientId,
            tool: "text",
            color: wbColor,
            size,
            x: nx,
            y: ny,
            text: text.trim().slice(0, 200)
          };
          wbHistory.push(stroke);
          redrawWhiteboard();
          sendSignal({ type: "wb_commit", stroke });
        }
        return;
      }

      wbCanvas.setPointerCapture?.(e.pointerId);
      wbIsDrawing = true;
      wbLastPoint = [nx, ny];

      const strokeId = `${clientId}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
      if (wbTool === "pen" || wbTool === "highlighter" || wbTool === "eraser") {
        wbCurrentStroke = { id: strokeId, sender: clientId, tool: wbTool, color: wbColor, size, points: [[nx, ny]] };
        drawSingleStroke(wbCtx, wbCurrentStroke);
      } else {
        wbCurrentStroke = { id: strokeId, sender: clientId, tool: wbTool, color: wbColor, size, x1: nx, y1: ny, x2: nx, y2: ny };
      }
    });

    wbCanvas.addEventListener("pointermove", (e) => {
      if (!wbIsDrawing || !wbCurrentStroke) return;
      e.preventDefault();
      const [nx, ny] = getNormalizedPoint(e);

      if (wbCurrentStroke.points) {
        const prev = wbLastPoint;
        wbCurrentStroke.points.push([nx, ny]);
        drawSingleStroke(wbCtx, {
          tool: wbCurrentStroke.tool,
          color: wbCurrentStroke.color,
          size: wbCurrentStroke.size,
          points: [prev, [nx, ny]]
        });
        wbLastPoint = [nx, ny];

        const now = Date.now();
        if (now - wbLastLiveSentAt > 35) {
          wbLastLiveSentAt = now;
          sendSignal({
            type: "wb_live",
            tool: wbCurrentStroke.tool,
            color: wbCurrentStroke.color,
            size: wbCurrentStroke.size,
            x1: prev[0], y1: prev[1], x2: nx, y2: ny
          });
        }
      } else {
        wbCurrentStroke.x2 = nx;
        wbCurrentStroke.y2 = ny;
        wbPreviewCtx.clearRect(0, 0, wbPreviewCanvas.width, wbPreviewCanvas.height);
        drawSingleStroke(wbPreviewCtx, wbCurrentStroke);
      }
    });

    function finishWhiteboardStroke() {
      if (!wbIsDrawing || !wbCurrentStroke) return;
      wbIsDrawing = false;
      wbPreviewCtx.clearRect(0, 0, wbPreviewCanvas.width, wbPreviewCanvas.height);
      wbHistory.push(wbCurrentStroke);
      redrawWhiteboard();
      sendSignal({ type: "wb_commit", stroke: wbCurrentStroke });
      wbCurrentStroke = null;
      wbLastPoint = null;
    }

    wbCanvas.addEventListener("pointerup", finishWhiteboardStroke);
    wbCanvas.addEventListener("pointercancel", finishWhiteboardStroke);

    wbUndoBtn.onclick = () => {
      if (!canDrawOnWhiteboard()) return;
      for (let i = wbHistory.length - 1; i >= 0; i--) {
        if (wbHistory[i].sender === clientId || isHost) {
          const [removed] = wbHistory.splice(i, 1);
          redrawWhiteboard();
          sendSignal({ type: "wb_undo", stroke_id: removed.id });
          break;
        }
      }
    };

    wbClearBtn.onclick = () => {
      if (!canDrawOnWhiteboard()) return;
      wbHistory = [];
      redrawWhiteboard();
      sendSignal({ type: "wb_clear" });
    };

    wbSaveBtn.onclick = () => {
      const a = document.createElement("a");
      a.href = wbCanvas.toDataURL("image/png");
      a.download = `whiteboard-${currentRoomId || "meeting"}.png`;
      a.click();
    };

    wbLockBtn.onclick = () => { if (isHost) sendSignal({ type: "wb_lock", locked: !wbLocked }); };

    wbToggleBtn.onclick = () => {
      wbOpen = !wbOpen;
      controlsDock.classList.remove("more-open");
      updateWhiteboardUI();
      if (wbOpen) redrawWhiteboard();
      sendSignal({ type: "wb_toggle", open: wbOpen });
    };

    wbCloseBtn.onclick = async () => {
      if (document.fullscreenElement === whiteboardStage || document.webkitFullscreenElement === whiteboardStage) {
        try {
          if (document.exitFullscreen) await document.exitFullscreen();
          else if (document.webkitExitFullscreen) await document.webkitExitFullscreen();
        } catch {}
      }
      wbOpen = false;
      updateWhiteboardUI();
      sendSignal({ type: "wb_toggle", open: false });
    };

    wbFullscreenBtn.onclick = async () => {
      const fsEl = document.fullscreenElement || document.webkitFullscreenElement;
      try {
        if (fsEl !== whiteboardStage) {
          if (whiteboardStage.requestFullscreen) await whiteboardStage.requestFullscreen();
          else if (whiteboardStage.webkitRequestFullscreen) await whiteboardStage.webkitRequestFullscreen();
        } else {
          if (document.exitFullscreen) await document.exitFullscreen();
          else if (document.webkitExitFullscreen) await document.webkitExitFullscreen();
        }
      } catch {}
    };

    redrawWhiteboard();

    // --- Active Speaker Lighting & Local Voice Monitor ---
    function startLocalVoiceMonitor(mediaStreamTrack) {
      stopLocalVoiceMonitor();
      if (!mediaStreamTrack) return;
      try {
        const AudioCtx = window.AudioContext || window.webkitAudioContext;
        localVadCtx = new AudioCtx();
        if (localVadCtx.state === "suspended") localVadCtx.resume().catch(() => {});
        const src = localVadCtx.createMediaStreamSource(new MediaStream([mediaStreamTrack]));
        const analyser = localVadCtx.createAnalyser();
        analyser.fftSize = 256;
        src.connect(analyser);
        const data = new Uint8Array(analyser.frequencyBinCount);

        localVadTimer = setInterval(() => {
          if (!micEnabled) {
            if (localMicSpeaking) { localMicSpeaking = false; updateSpeakingUI(); }
            return;
          }
          analyser.getByteFrequencyData(data);
          let sum = 0;
          for (let i = 0; i < data.length; i++) sum += data[i];
          const avg = sum / data.length;

          if (avg > 12) {
            clearTimeout(localHoldTimer);
            localHoldTimer = null;
            if (!localMicSpeaking) { localMicSpeaking = true; updateSpeakingUI(); }
          } else if (localMicSpeaking && !localHoldTimer) {
            localHoldTimer = setTimeout(() => {
              localHoldTimer = null;
              localMicSpeaking = false;
              updateSpeakingUI();
            }, 450);
          }
        }, 120);
      } catch {}
    }

    function stopLocalVoiceMonitor() {
      if (localVadTimer) { clearInterval(localVadTimer); localVadTimer = null; }
      clearTimeout(localHoldTimer);
      localHoldTimer = null;
      if (localVadCtx) { localVadCtx.close().catch(() => {}); localVadCtx = null; }
      localMicSpeaking = false;
      updateSpeakingUI();
    }

    function updateSpeakingUI() {
      const speakingSet = new Set(lkSpeakingIds);
      if (localMicSpeaking && micEnabled) speakingSet.add("local");

      for (const id of speakingSet) {
        if (id !== "local") { lastActiveSpeakerTileId = id; break; }
      }
      if (!lastActiveSpeakerTileId && speakingSet.has("local")) {
        lastActiveSpeakerTileId = "local";
      }

      document.querySelectorAll("#video-grid .video-tile").forEach(tile => {
        const tid = tile.id.replace("tile-", "");
        tile.classList.toggle("speaking", speakingSet.has(tid));
      });

      document.querySelectorAll("#participants-list .participant-row[data-pid]").forEach(row => {
        const pid = row.dataset.pid;
        const tileKey = pid === clientId ? "local" : pid;
        row.classList.toggle("speaking", speakingSet.has(tileKey));
      });

      if ((currentViewMode === "speaker" || currentViewMode === "sidebar") && !pinnedTileId) {
        updateFeaturedTile();
      }
    }

    // --- Meeting View Switcher + Mobile Touch Swipe ---
    function getPageSize() { return currentViewMode === "compact" ? 24 : 12; }

    function showSwipeViewToast(mode) {
      viewSwipeToast.textContent = VIEW_LABELS[mode] || mode;
      viewSwipeToast.classList.remove("hidden");
      clearTimeout(swipeToastTimer);
      swipeToastTimer = setTimeout(() => {
        viewSwipeToast.classList.add("hidden");
      }, 1400);
    }

    function applyViewMode(mode, swipeDirection = null) {
      if (!VIEW_MODES.includes(mode)) mode = "grid";
      currentViewMode = mode;
      viewModeSelect.value = mode;
      try { localStorage.setItem("vc_view_mode", mode); } catch {}

      document.querySelectorAll("#mobile-view-bar .m-view-pill").forEach(pill => {
        pill.classList.toggle("active-pill", pill.dataset.mode === mode);
      });

      videoGrid.classList.remove("mode-grid", "mode-speaker", "mode-sidebar", "mode-compact", "swipe-next", "swipe-prev");
      videoGrid.classList.add(`mode-${mode}`);
      videoGrid.classList.toggle("single-tile", tileOrder.length <= 1);

      if (swipeDirection === "next" || swipeDirection === "prev") {
        void videoGrid.offsetWidth;
        videoGrid.classList.add(swipeDirection === "next" ? "swipe-next" : "swipe-prev");
        showSwipeViewToast(mode);
      }

      updateFeaturedTile();
      renderPagination();
    }

    videoGrid.addEventListener("animationend", (e) => {
      if (e.target === videoGrid) {
        videoGrid.classList.remove("swipe-next", "swipe-prev");
      }
    });

    let touchStartX = 0;
    let touchStartY = 0;
    let touchStartTime = 0;
    let trackingSwipe = false;

    videoGrid.addEventListener("touchstart", (e) => {
      if (e.touches.length !== 1 || e.target.closest(".pin-btn")) {
        trackingSwipe = false;
        return;
      }
      touchStartX = e.touches[0].clientX;
      touchStartY = e.touches[0].clientY;
      touchStartTime = Date.now();
      trackingSwipe = true;
    }, { passive: true });

    videoGrid.addEventListener("touchend", (e) => {
      if (!trackingSwipe || !e.changedTouches.length) return;
      trackingSwipe = false;

      const dx = e.changedTouches[0].clientX - touchStartX;
      const dy = e.changedTouches[0].clientY - touchStartY;
      const dt = Date.now() - touchStartTime;

      if (dt < 650 && Math.abs(dx) > 55 && Math.abs(dx) > Math.abs(dy) * 1.45) {
        const idx = VIEW_MODES.indexOf(currentViewMode);
        if (dx < 0) {
          applyViewMode(VIEW_MODES[(idx + 1) % VIEW_MODES.length], "next");
        } else {
          applyViewMode(VIEW_MODES[(idx - 1 + VIEW_MODES.length) % VIEW_MODES.length], "prev");
        }
      }
    }, { passive: true });

    document.querySelectorAll("#mobile-view-bar .m-view-pill").forEach(pill => {
      pill.onclick = () => {
        const targetMode = pill.dataset.mode;
        if (targetMode === currentViewMode) return;
        const oldIdx = VIEW_MODES.indexOf(currentViewMode);
        const newIdx = VIEW_MODES.indexOf(targetMode);
        applyViewMode(targetMode, newIdx > oldIdx ? "next" : "prev");
      };
    });

    function updateFeaturedTile() {
      document.querySelectorAll("#video-grid .video-tile").forEach(tile => {
        tile.classList.remove("featured-tile");
        const pb = tile.querySelector(".pin-btn");
        if (pb) {
          const isPinned = tile.id === `tile-${pinnedTileId}`;
          pb.classList.toggle("pinned", isPinned);
          pb.textContent = isPinned ? "📌 Pinned" : "📌 Pin";
        }
      });

      if (currentViewMode !== "speaker" && currentViewMode !== "sidebar") return;

      let targetId = null;
      if (pinnedTileId && document.getElementById(`tile-${pinnedTileId}`)) {
        targetId = pinnedTileId;
      } else if (lastActiveSpeakerTileId && document.getElementById(`tile-${lastActiveSpeakerTileId}`)) {
        targetId = lastActiveSpeakerTileId;
      } else {
        targetId = tileOrder.find(id => id !== "local") || "local";
      }

      const targetTile = document.getElementById(`tile-${targetId}`);
      if (targetTile) {
        targetTile.classList.add("featured-tile");
        targetTile.classList.remove("page-hidden");
      }
    }

    viewModeSelect.value = currentViewMode;
    viewModeSelect.onchange = () => {
      applyViewMode(viewModeSelect.value);
      controlsDock.classList.remove("more-open");
    };

    // --- Notification Sounds ---
    function playChime(notes) {
      try {
        const AudioCtx = window.AudioContext || window.webkitAudioContext;
        if (!AudioCtx) return;
        const ctx = new AudioCtx();
        if (ctx.state === "suspended") ctx.resume().catch(() => {});

        let now = ctx.currentTime;
        notes.forEach(([freq, dur], idx) => {
          const osc = ctx.createOscillator();
          const gain = ctx.createGain();
          osc.type = "sine";
          osc.frequency.setValueAtTime(freq, now + idx * 0.11);
          gain.gain.setValueAtTime(0.001, now + idx * 0.11);
          gain.gain.exponentialRampToValueAtTime(0.14, now + idx * 0.11 + 0.02);
          gain.gain.exponentialRampToValueAtTime(0.001, now + idx * 0.11 + dur);
          osc.connect(gain);
          gain.connect(ctx.destination);
          osc.start(now + idx * 0.11);
          osc.stop(now + idx * 0.11 + dur + 0.02);
        });
        setTimeout(() => ctx.close().catch(() => {}), 800);
      } catch {}
    }

    function playJoinSound() { playChime([[523.25, 0.16], [659.25, 0.24]]); }
    function playWaitingKnockSound() { playChime([[440, 0.14], [554.37, 0.14], [659.25, 0.25]]); }

    // --- Helpers ---
    function getMeetingUrl(roomId) { return `${location.origin}/room/${encodeURIComponent(roomId)}`; }

    function escapeHtml(str) {
      return String(str).replace(/[&<>"']/g, s => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
      }[s]));
    }

    async function copyText(text, btn) {
      try {
        await navigator.clipboard.writeText(text);
        if (btn) {
          const origHtml = btn.innerHTML;
          btn.textContent = "Copied!";
          setTimeout(() => btn.innerHTML = origHtml, 1500);
        }
      } catch {
        prompt("Copy this meeting link:", text);
      }
    }

    function formatDateTime(iso) {
      if (!iso) return "Instant / Unscheduled";
      const d = new Date(iso);
      return isNaN(d) ? iso : d.toLocaleString();
    }

    // --- Participants Panel ---
    function getRemoteMediaStatus(pid) {
      if (!lkRoom) return { mic: false, cam: false };
      const p = lkRoom.remoteParticipants.get(pid);
      if (!p) return { mic: false, cam: false };
      const micPub = p.getTrackPublication(LivekitClient.Track.Source.Microphone);
      const camPub = p.getTrackPublication(LivekitClient.Track.Source.Camera);
      return {
        mic: Boolean(micPub && !micPub.isMuted),
        cam: Boolean(camPub && !camPub.isMuted)
      };
    }

    function updateParticipantCounters() {
      const inMeetingCount = Object.keys(peerNames).length + 1;
      participantCountEl.textContent = `👥 ${inMeetingCount} / ${maxUsersInRoom}`;
      participantsBtnCount.textContent = inMeetingCount;
      participantsPanelTitle.textContent = `Participants (${inMeetingCount} / ${maxUsersInRoom})`;

      const effectiveWaiting = isHost ? Object.keys(waitingUsers).length : waitingCount;
      joiningCountPill.classList.toggle("hidden", effectiveWaiting === 0);
      joiningCountPill.textContent = `⏳ ${effectiveWaiting} joining`;

      joiningBtnBadge.classList.toggle("hidden", effectiveWaiting === 0);
      joiningBtnBadge.textContent = `${effectiveWaiting}`;
    }

    function renderParticipantsList() {
      updateParticipantCounters();
      const filter = participantSearch.value.trim().toLowerCase();
      participantsList.innerHTML = "";

      const waitingEntries = Object.values(waitingUsers);
      const effectiveWaiting = isHost ? waitingEntries.length : waitingCount;

      if (effectiveWaiting > 0) {
        const waitHeader = document.createElement("div");
        waitHeader.className = "p-section-title";
        waitHeader.innerHTML = `<span>⏳ Joining / Waiting Room (${effectiveWaiting})</span>`;

        if (isHost && waitingEntries.length > 0) {
          const admitAllBtn = document.createElement("button");
          admitAllBtn.className = "active";
          admitAllBtn.style.cssText = "padding: 0.2rem 0.55rem; font-size: 0.72rem;";
          admitAllBtn.textContent = "Admit All";
          admitAllBtn.onclick = () => {
            waitingEntries.forEach(u => {
              const card = document.getElementById(`wait-${u.id}`);
              if (card) card.remove();
            });
            waitingUsers = {};
            waitingCount = 0;
            sendSignal({ type: "admit_all" });
            renderParticipantsList();
          };
          waitHeader.appendChild(admitAllBtn);
        }
        participantsList.appendChild(waitHeader);

        if (isHost) {
          for (const u of waitingEntries) {
            if (filter && !u.name.toLowerCase().includes(filter)) continue;
            const row = document.createElement("div");
            row.className = "participant-row";
            row.style.borderColor = "#d97706";

            const left = document.createElement("div");
            left.className = "participant-left";
            const av = document.createElement("div");
            av.className = "avatar-circle small";
            renderAvatarCircle(av, u.avatar, u.name);
            const nw = document.createElement("div");
            nw.className = "participant-name-wrap";
            nw.innerHTML = `
              <span class="participant-name">${escapeHtml(u.name)}</span>
              <span class="participant-tags"><span class="mini-tag" style="background:#b45309;color:#fff;">Waiting</span></span>
            `;
            left.appendChild(av);
            left.appendChild(nw);

            const right = document.createElement("div");
            right.className = "participant-right";
            right.innerHTML = `
              <button class="active p-admit-btn" style="padding:0.28rem 0.55rem;font-size:0.75rem;">Admit</button>
              <button class="danger p-decline-btn" style="padding:0.28rem 0.55rem;font-size:0.75rem;">✕</button>
            `;
            right.querySelector(".p-admit-btn").onclick = () => {
              delete waitingUsers[u.id];
              const card = document.getElementById(`wait-${u.id}`);
              if (card) card.remove();
              sendSignal({ type: "join_decision", target: u.id, approved: true });
              renderParticipantsList();
            };
            right.querySelector(".p-decline-btn").onclick = () => {
              delete waitingUsers[u.id];
              const card = document.getElementById(`wait-${u.id}`);
              if (card) card.remove();
              sendSignal({ type: "join_decision", target: u.id, approved: false });
              renderParticipantsList();
            };

            row.appendChild(left);
            row.appendChild(right);
            participantsList.appendChild(row);
          }
        } else {
          const infoBox = document.createElement("div");
          infoBox.className = "waiting-banner-box";
          infoBox.textContent = `${effectiveWaiting} participant(s) are currently in the waiting room waiting for host approval.`;
          participantsList.appendChild(infoBox);
        }
      }

      const allParticipants = [
        {
          id: clientId,
          name: getDisplayName(),
          avatar: localAvatar,
          isSelf: true,
          isHost: isHost,
          isPresenting: isSharingScreen,
          mic: micEnabled,
          cam: camEnabled
        },
        ...Object.entries(peerNames).map(([id, name]) => {
          const status = getRemoteMediaStatus(id);
          return {
            id,
            name,
            avatar: peerAvatars[id] || "",
            isSelf: false,
            isHost: id === currentHostId,
            isPresenting: id === activeSharer,
            mic: status.mic,
            cam: status.cam
          };
        })
      ];

      const inHeader = document.createElement("div");
      inHeader.className = "p-section-title";
      inHeader.textContent = `In Meeting (${allParticipants.length})`;
      participantsList.appendChild(inHeader);

      const speakingSet = new Set(lkSpeakingIds);
      if (localMicSpeaking && micEnabled) speakingSet.add("local");

      for (const p of allParticipants) {
        if (filter && !p.name.toLowerCase().includes(filter)) continue;
        const row = document.createElement("div");
        const tileKey = p.isSelf ? "local" : p.id;
        row.className = `participant-row${speakingSet.has(tileKey) ? " speaking" : ""}`;
        row.dataset.pid = p.id;

        const left = document.createElement("div");
        left.className = "participant-left";
        const av = document.createElement("div");
        av.className = "avatar-circle small";
        renderAvatarCircle(av, p.avatar, p.name);

        const tags = [];
        if (p.isSelf) tags.push(`<span class="mini-tag">You</span>`);
        if (p.isHost) tags.push(`<span class="mini-tag host">Host</span>`);
        if (p.isPresenting) tags.push(`<span class="mini-tag screen">Presenting</span>`);

        const nw = document.createElement("div");
        nw.className = "participant-name-wrap";
        nw.innerHTML = `
          <span class="participant-name">${escapeHtml(p.name)}</span>
          ${tags.length ? `<div class="participant-tags">${tags.join("")}</div>` : ""}
        `;
        left.appendChild(av);
        left.appendChild(nw);

        const right = document.createElement("div");
        right.className = "participant-right";
        right.innerHTML = `
          <span title="${p.mic ? "Microphone On" : "Microphone Off"}">${p.mic ? "🎤" : "🔇"}</span>
          <span title="${p.cam ? "Camera On" : "Camera Off"}">${p.cam ? "📷" : "🚫"}</span>
        `;

        if (!p.isSelf) {
          const dmBtn = document.createElement("button");
          dmBtn.className = "secondary";
          dmBtn.style.cssText = "padding:0.25rem 0.5rem; font-size:0.75rem;";
          dmBtn.textContent = "DM";
          dmBtn.title = `Send private message to ${p.name}`;
          dmBtn.onclick = () => {
            participantsPanel.classList.add("hidden");
            chatPanel.classList.remove("hidden");
            switchChatTab("private");
            dmTarget.value = p.id;
            chatInput.focus();
          };
          right.appendChild(dmBtn);
        }

        row.appendChild(left);
        row.appendChild(right);
        participantsList.appendChild(row);
      }
    }

    participantSearch.addEventListener("input", renderParticipantsList);

    toggleParticipantsBtn.onclick = () => {
      const opening = participantsPanel.classList.contains("hidden");
      participantsPanel.classList.toggle("hidden", !opening);
      if (opening) {
        chatPanel.classList.add("hidden");
        controlsDock.classList.remove("more-open");
        renderParticipantsList();
      }
      updateUnreadBadges();
    };

    closeParticipantsBtn.onclick = () => { participantsPanel.classList.add("hidden"); };
    closeChatBtn.onclick = () => { chatPanel.classList.add("hidden"); updateUnreadBadges(); };

    // --- Recording Player Modal ---
    function openRecordingPlayer(title, url) {
      playerTitle.textContent = title;
      playerVideo.src = url;
      playerModal.classList.remove("hidden");
    }

    playerCloseBtn.onclick = () => {
      playerVideo.pause();
      playerVideo.removeAttribute("src");
      playerVideo.load();
      playerModal.classList.add("hidden");
    };

    // --- Fullscreen + Locked HD Quality ---
    function isScreenFullscreen() {
      const fsEl = document.fullscreenElement || document.webkitFullscreenElement;
      return fsEl === screenStage || fsEl === screenStageVideo;
    }

    function applyHighQualityScreenShare() {
      if (!activeScreenPublication) return;
      try {
        activeScreenPublication.setVideoQuality?.(LivekitClient.VideoQuality.HIGH);
        const dpr = window.devicePixelRatio || 1;
        const targetWidth = isScreenFullscreen()
          ? Math.max(1920, Math.round(window.screen.width * dpr))
          : 1920;
        const targetHeight = isScreenFullscreen()
          ? Math.max(1080, Math.round(window.screen.height * dpr))
          : 1080;
        activeScreenPublication.setVideoDimensions?.({ width: targetWidth, height: targetHeight });
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
      } catch {
        if (screenStageVideo.webkitEnterFullscreen) {
          try { screenStageVideo.webkitEnterFullscreen(); } catch {}
        }
      }
    }

    async function exitScreenFullscreenIfActive() {
      if (isScreenFullscreen()) {
        try {
          if (document.exitFullscreen) await document.exitFullscreen();
          else if (document.webkitExitFullscreen) await document.webkitExitFullscreen();
        } catch {}
      }
    }

    function onFullscreenChange() {
      screenFullscreenBtn.textContent = isScreenFullscreen() ? "✕ Exit Fullscreen" : "⛶ Fullscreen";
      applyHighQualityScreenShare();
      const fsEl = document.fullscreenElement || document.webkitFullscreenElement;
      wbFullscreenBtn.textContent = fsEl === whiteboardStage ? "✕ Exit" : "⛶ Fullscreen";
    }

    screenFullscreenBtn.onclick = toggleScreenFullscreen;
    screenStageVideo.ondblclick = toggleScreenFullscreen;
    document.addEventListener("fullscreenchange", onFullscreenChange);
    document.addEventListener("webkitfullscreenchange", onFullscreenChange);

    // --- Banners ---
    function showNoticeBanner(text, variant = "", durationMs = 4500) {
      const card = document.createElement("div");
      card.className = `banner-card ${variant}`;
      card.innerHTML = `<span class="banner-text">${escapeHtml(text)}</span>`;
      bannerContainer.appendChild(card);
      if (durationMs > 0) setTimeout(() => card.remove(), durationMs);
      return card;
    }

    function showWaitingUserBanner(user) {
      const existing = document.getElementById(`wait-${user.id}`);
      if (existing) existing.remove();

      const card = document.createElement("div");
      card.id = `wait-${user.id}`;
      card.className = "banner-card waiting-req";

      const textSpan = document.createElement("span");
      textSpan.className = "banner-text";

      const avatarEl = document.createElement("div");
      avatarEl.className = "avatar-circle small";
      renderAvatarCircle(avatarEl, user.avatar, user.name);

      const labelEl = document.createElement("span");
      labelEl.innerHTML = `<b>${escapeHtml(user.name)}</b> is in the waiting room`;

      textSpan.appendChild(avatarEl);
      textSpan.appendChild(labelEl);

      const actions = document.createElement("div");
      actions.className = "row";
      actions.style.flexShrink = "0";
      actions.innerHTML = `
        <button class="active admit-btn" style="padding:0.4rem 0.7rem;">Admit</button>
        <button class="danger deny-btn" style="padding:0.4rem 0.7rem;">Decline</button>
      `;

      actions.querySelector(".admit-btn").onclick = () => {
        delete waitingUsers[user.id];
        sendSignal({ type: "join_decision", target: user.id, approved: true });
        card.remove();
        renderParticipantsList();
      };
      actions.querySelector(".deny-btn").onclick = () => {
        delete waitingUsers[user.id];
        sendSignal({ type: "join_decision", target: user.id, approved: false });
        card.remove();
        renderParticipantsList();
      };

      card.appendChild(textSpan);
      card.appendChild(actions);
      bannerContainer.appendChild(card);
    }

    function showHostRequestBanner(requesterId, requesterName) {
      const existing = document.getElementById(`req-${requesterId}`);
      if (existing) existing.remove();

      const card = document.createElement("div");
      card.id = `req-${requesterId}`;
      card.className = "banner-card";
      card.innerHTML = `
        <span class="banner-text">🔔 <b>${escapeHtml(requesterName)}</b> wants to share their screen</span>
        <div class="row" style="flex-shrink:0;">
          <button class="active allow-btn" style="padding:0.4rem 0.7rem;">Allow</button>
          <button class="danger deny-btn" style="padding:0.4rem 0.7rem;">Deny</button>
        </div>
      `;
      card.querySelector(".allow-btn").onclick = () => {
        sendSignal({ type: "screen_share_decision", target: requesterId, approved: true });
        card.remove();
      };
      card.querySelector(".deny-btn").onclick = () => {
        sendSignal({ type: "screen_share_decision", target: requesterId, approved: false });
        card.remove();
      };
      bannerContainer.appendChild(card);
    }

    function showApprovedBanner() {
      const existing = document.getElementById("approved-banner");
      if (existing) existing.remove();

      const card = document.createElement("div");
      card.id = "approved-banner";
      card.className = "banner-card approved";
      card.innerHTML = `
        <span class="banner-text">✅ Host approved your screen share!</span>
        <div class="row" style="flex-shrink:0;">
          <button class="active start-approved-btn" style="padding:0.4rem 0.75rem;">Share Now</button>
          <button class="secondary dismiss-btn" style="padding:0.4rem 0.6rem;">✕</button>
        </div>
      `;
      card.querySelector(".start-approved-btn").onclick = async () => {
        card.remove();
        await launchDisplayMedia();
      };
      card.querySelector(".dismiss-btn").onclick = () => card.remove();
      bannerContainer.appendChild(card);
    }

    // --- Smooth Host Meeting Recorder ---
    function attachAudioTrackToRecorder(mediaStreamTrack) {
      if (!isRecordingLocally || !recAudioCtx || !recAudioDest || !mediaStreamTrack) return;
      if (recConnectedTrackIds.has(mediaStreamTrack.id)) return;
      try {
        const src = recAudioCtx.createMediaStreamSource(new MediaStream([mediaStreamTrack]));
        src.connect(recAudioDest);
        recConnectedTrackIds.add(mediaStreamTrack.id);
      } catch {}
    }

    function syncAllAudioToRecorder() {
      if (!isRecordingLocally || !lkRoom) return;
      const localMic = lkRoom.localParticipant.getTrackPublication(LivekitClient.Track.Source.Microphone);
      if (localMic?.track?.mediaStreamTrack) {
        attachAudioTrackToRecorder(localMic.track.mediaStreamTrack);
      }
      for (const participant of lkRoom.remoteParticipants.values()) {
        for (const pub of participant.trackPublications.values()) {
          if (pub.isSubscribed && pub.track?.kind === LivekitClient.Track.Kind.Audio && pub.track.mediaStreamTrack) {
            attachAudioTrackToRecorder(pub.track.mediaStreamTrack);
          }
        }
      }
    }

    function drawRecorderFrame(ctx) {
      ctx.fillStyle = "#0f1115";
      ctx.fillRect(0, 0, recCanvas.width, recCanvas.height);

      if (!screenStage.classList.contains("hidden") && !screenStageVideo.classList.contains("hidden") && screenStageVideo.videoWidth > 0) {
        drawContained(ctx, screenStageVideo, 0, 0, recCanvas.width, recCanvas.height);
        return;
      }

      if (isSharingScreen && localScreenMirrorVideo && localScreenMirrorVideo.videoWidth > 0) {
        drawContained(ctx, localScreenMirrorVideo, 0, 0, recCanvas.width, recCanvas.height);
        return;
      }

      if (wbOpen && !whiteboardStage.classList.contains("hidden")) {
        drawContained(ctx, wbCanvas, 0, 0, recCanvas.width, recCanvas.height);
        return;
      }

      const tiles = Array.from(videoGrid.querySelectorAll(".video-tile:not(.page-hidden)"));
      const count = Math.max(1, tiles.length);
      const cols = Math.ceil(Math.sqrt(count));
      const rows = Math.ceil(count / cols);
      const cellW = recCanvas.width / cols;
      const cellH = recCanvas.height / rows;

      tiles.forEach((tile, i) => {
        const col = i % cols;
        const row = Math.floor(i / cols);
        const x = col * cellW;
        const y = row * cellH;

        ctx.fillStyle = "#181b22";
        ctx.fillRect(x + 4, y + 4, cellW - 8, cellH - 8);

        const vid = tile.querySelector("video");
        const placeholder = tile.querySelector(".placeholder");
        const isPlaceholderShown = placeholder && !placeholder.classList.contains("hidden");

        if (vid && !isPlaceholderShown && vid.videoWidth > 0) {
          drawContained(ctx, vid, x + 4, y + 4, cellW - 8, cellH - 8);
        } else {
          ctx.fillStyle = "#a1a1aa";
          ctx.font = "20px system-ui, sans-serif";
          ctx.textAlign = "center";
          ctx.fillText(placeholder?.querySelector(".ph-text")?.textContent || "Camera Off", x + cellW / 2, y + cellH / 2);
        }

        const label = tile.querySelector(".label")?.textContent || "";
        ctx.fillStyle = "rgba(0,0,0,0.7)";
        ctx.fillRect(x + 12, y + cellH - 36, Math.min(cellW - 24, 240), 26);
        ctx.fillStyle = "#fff";
        ctx.font = "14px system-ui, sans-serif";
        ctx.textAlign = "left";
        ctx.fillText(label, x + 18, y + cellH - 18);
      });
    }

    function fixWebmSeekable(rawBlob, durationMs) {
      return new Promise((resolve) => {
        if (typeof ysFixWebmDuration === "function") {
          try {
            ysFixWebmDuration(rawBlob, durationMs, (fixedBlob) => resolve(fixedBlob || rawBlob), { logger: false });
            return;
          } catch {}
        }
        resolve(rawBlob);
      });
    }

    async function startHostRecording() {
      if (!isHost || isRecordingLocally) return;
      const saveRoomId = currentRoomId || "meeting";

      try {
        recCanvas = document.createElement("canvas");
        recCanvas.width = 1280;
        recCanvas.height = 720;
        const ctx = recCanvas.getContext("2d", { alpha: false, desynchronized: true });

        recAudioCtx = new (window.AudioContext || window.webkitAudioContext)();
        if (recAudioCtx.state === "suspended") await recAudioCtx.resume();
        recAudioDest = recAudioCtx.createMediaStreamDestination();

        recSilentOsc = recAudioCtx.createOscillator();
        const silentGain = recAudioCtx.createGain();
        silentGain.gain.value = 0;
        recSilentOsc.connect(silentGain);
        silentGain.connect(recAudioDest);
        recSilentOsc.start();

        isRecordingLocally = true;
        recConnectedTrackIds = new Set();
        syncAllAudioToRecorder();

        drawRecorderFrame(ctx);

        try {
          const workerCode = `
            let t = null;
            self.onmessage = (e) => {
              if (e.data === "start") { clearInterval(t); t = setInterval(() => self.postMessage(0), 33); }
              else if (e.data === "stop") { clearInterval(t); t = null; }
            };
          `;
          recWorkerUrl = URL.createObjectURL(new Blob([workerCode], { type: "application/javascript" }));
          recWorker = new Worker(recWorkerUrl);
          recWorker.onmessage = () => drawRecorderFrame(ctx);
          recWorker.postMessage("start");
        } catch {
          recDrawTimer = setInterval(() => drawRecorderFrame(ctx), 33);
        }

        const canvasStream = recCanvas.captureStream(30);
        const combinedStream = new MediaStream([
          ...canvasStream.getVideoTracks(),
          ...recAudioDest.stream.getAudioTracks()
        ]);

        let mimeType = "";
        for (const c of ["video/webm;codecs=h264,opus", "video/webm;codecs=vp8,opus", "video/webm;codecs=vp9,opus", "video/webm"]) {
          if (MediaRecorder.isTypeSupported(c)) { mimeType = c; break; }
        }

        recordedChunks = [];
        try {
          mediaRecorder = mimeType
            ? new MediaRecorder(combinedStream, { mimeType, videoBitsPerSecond: 3_500_000 })
            : new MediaRecorder(combinedStream);
        } catch {
          mediaRecorder = new MediaRecorder(combinedStream);
        }

        mediaRecorder.ondataavailable = (e) => {
          if (e.data && e.data.size > 0) recordedChunks.push(e.data);
        };

        recStopPromise = new Promise((resolve) => {
          mediaRecorder.onstop = async () => {
            const totalDurationMs = Math.max(500, Date.now() - recStartTime);
            if (recWorker) { recWorker.postMessage("stop"); recWorker.terminate(); recWorker = null; }
            if (recWorkerUrl) { URL.revokeObjectURL(recWorkerUrl); recWorkerUrl = null; }
            if (recDrawTimer) { clearInterval(recDrawTimer); recDrawTimer = null; }
            canvasStream.getTracks().forEach(t => t.stop());

            if (recSilentOsc) { try { recSilentOsc.stop(); } catch {} recSilentOsc = null; }
            if (recAudioCtx) { recAudioCtx.close().catch(() => {}); recAudioCtx = null; }
            recAudioDest = null;
            recConnectedTrackIds.clear();

            if (recordedChunks.length === 0) {
              showNoticeBanner("⚠️ Recording stopped but captured 0 bytes.", "denied", 4500);
              resolve();
              return;
            }

            const finalType = mediaRecorder?.mimeType || mimeType || "video/webm";
            const rawBlob = new Blob(recordedChunks, { type: finalType });
            recordedChunks = [];

            const fixedBlob = await fixWebmSeekable(rawBlob, totalDurationMs);

            let serverSavedMsg = "";
            try {
              const hk = localStorage.getItem(`vc_host_key_${saveRoomId}`) || "";
              const resp = await fetch(`/api/recordings/${encodeURIComponent(saveRoomId)}`, {
                method: "POST",
                headers: {
                  "Content-Type": "application/octet-stream",
                  "X-Host-Key": hk
                },
                body: fixedBlob
              });
              if (resp.ok) {
                const info = await resp.json();
                serverSavedMsg = `Saved to recordings/${info.filename} & Downloads!`;
              }
            } catch {}

            const url = URL.createObjectURL(fixedBlob);
            const a = document.createElement("a");
            const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
            a.href = url;
            a.download = `meeting-${saveRoomId}-${stamp}.webm`;
            document.body.appendChild(a);
            a.click();
            a.remove();
            setTimeout(() => URL.revokeObjectURL(url), 5000);

            showNoticeBanner(
              `💾 ${serverSavedMsg || "Seekable recording saved to your Downloads folder!"}`,
              "approved",
              6000
            );
            if (isMainHostServer) loadRecordings();
            resolve();
          };
        });

        recStartTime = Date.now();
        mediaRecorder.start(2000);
        isMeetingRecording = true;
        sendSignal({ type: "recording_started" });
        updateRecordingUI(true, getDisplayName());
        updateControlButtons();
      } catch (err) {
        isRecordingLocally = false;
        showNoticeBanner(`❌ Could not start recording: ${err.message}`, "denied");
      }
    }

    async function stopHostRecording(notifyServer = true) {
      if (!isRecordingLocally) return;
      isRecordingLocally = false;
      isMeetingRecording = false;
      if (notifyServer) sendSignal({ type: "recording_stopped" });
      updateRecordingUI(false);
      updateControlButtons();

      if (mediaRecorder && mediaRecorder.state !== "inactive") {
        try { mediaRecorder.requestData(); } catch {}
        mediaRecorder.stop();
        if (recStopPromise) await recStopPromise;
      }
      mediaRecorder = null;
      recStopPromise = null;
    }

    function drawContained(ctx, sourceEl, dx, dy, dw, dh) {
      const vw = sourceEl.videoWidth || sourceEl.width;
      const vh = sourceEl.videoHeight || sourceEl.height;
      if (!vw || !vh) return;
      const scale = Math.min(dw / vw, dh / vh);
      const w = vw * scale;
      const h = vh * scale;
      ctx.drawImage(sourceEl, dx + (dw - w) / 2, dy + (dh - h) / 2, w, h);
    }

    function updateRecordingUI(recording, byName = "Host") {
      isMeetingRecording = recording;
      recordingPill.classList.toggle("hidden", !recording);
      recordingPill.textContent = `● REC`;
      recordingPill.title = recording ? `Recording by ${byName}` : "";
    }

    // --- Auto Captions ---
    function displayCaption(speaker, text) {
      if (!captionsEnabled) return;
      captionBar.classList.remove("hidden");
      captionSpeaker.textContent = `${speaker}:`;
      captionText.textContent = text;
      clearTimeout(captionClearTimer);
      captionClearTimer = setTimeout(() => {
        if (captionsEnabled) {
          captionSpeaker.textContent = "";
          captionText.textContent = "Listening for speech...";
        }
      }, 5000);
    }

    function startSpeechRecognition() {
      if (!captionsEnabled || !micEnabled) return;
      const SpeechRec = window.SpeechRecognition || window.webkitSpeechRecognition;
      if (!SpeechRec) {
        showNoticeBanner("⚠️ Live speech transcription is not supported in this browser (use Chrome or Edge).", "warning", 5000);
        return;
      }
      if (speechRecognition) return;

      const rec = new SpeechRec();
      rec.continuous = true;
      rec.interimResults = true;
      rec.lang = navigator.language || "en-US";

      rec.onresult = (event) => {
        const result = event.results[event.results.length - 1];
        const transcript = result[0].transcript.trim();
        if (!transcript) return;
        displayCaption(`${getDisplayName()} (You)`, transcript);
        const now = Date.now();
        if (result.isFinal || now - lastCaptionSentAt > 350) {
          lastCaptionSentAt = now;
          sendSignal({ type: "caption", text: transcript, is_final: result.isFinal });
        }
      };

      rec.onerror = (event) => {
        if (event.error === "network") {
          stopSpeechRecognition();
          showNoticeBanner("⚠️ Browser blocked speech-to-text (common in Brave; use Chrome/Edge to transcribe your mic).", "denied", 6000);
        } else if (event.error === "not-allowed" || event.error === "service-not-allowed") {
          stopSpeechRecognition();
        }
      };

      rec.onend = () => {
        speechRecognition = null;
        if (captionsEnabled && micEnabled && hasJoinedRoom) {
          setTimeout(() => startSpeechRecognition(), 300);
        }
      };

      try { rec.start(); speechRecognition = rec; } catch {}
    }

    function stopSpeechRecognition() {
      if (speechRecognition) {
        const rec = speechRecognition;
        speechRecognition = null;
        try { rec.onend = null; rec.stop(); } catch {}
      }
    }

    function toggleAutoCaptions() {
      captionsEnabled = !captionsEnabled;
      if (captionsEnabled) {
        captionBar.classList.remove("hidden");
        captionSpeaker.textContent = "";
        captionText.textContent = micEnabled
          ? "Auto Captions On (listening...)"
          : "Auto Captions On (switch to Microphone to caption your voice)";
        if (micEnabled) startSpeechRecognition();
      } else {
        stopSpeechRecognition();
        clearTimeout(captionClearTimer);
        captionBar.classList.add("hidden");
      }
      updateControlButtons();
    }

    ccBtn.onclick = () => {
      toggleAutoCaptions();
      controlsDock.classList.remove("more-open");
    };

    // --- Scheduler, Recordings & Meeting APIs (Main Host Server Only) ---
    async function loadRecordings() {
      if (!isMainHostServer) return;
      try {
        const res = await fetch("/api/recordings");
        if (!res.ok) return;
        const recs = await res.json();
        recordingList.innerHTML = "";
        if (recs.length === 0) {
          recordingList.innerHTML = `<div class="meeting-sub">No recordings saved yet.</div>`;
          return;
        }
        for (const r of recs) {
          const item = document.createElement("div");
          item.className = "meeting-item";
          item.innerHTML = `
            <div class="meeting-meta">
              <span class="meeting-title">🎬 ${escapeHtml(r.filename)}</span>
              <span class="meeting-sub">${r.size_mb} MB</span>
            </div>
            <div class="row">
              <button class="secondary watch-rec-btn">Watch</button>
              <a class="btn-link" href="${escapeHtml(r.url)}?download=1" download="${escapeHtml(r.filename)}">Download</a>
            </div>
          `;
          item.querySelector(".watch-rec-btn").onclick = () => openRecordingPlayer(r.filename, r.url);
          recordingList.appendChild(item);
        }
      } catch {
        recordingList.innerHTML = `<div class="meeting-sub">Could not load recordings.</div>`;
      }
    }

    async function loadMeetings() {
      if (!isMainHostServer) return;
      try {
        const res = await fetch("/api/meetings");
        if (!res.ok) return;
        const meetings = await res.json();
        meetingList.innerHTML = "";
        if (meetings.length === 0) {
          meetingList.innerHTML = `<div class="meeting-sub">No meetings scheduled yet.</div>`;
          return;
        }
        for (const m of meetings) {
          const item = document.createElement("div");
          item.className = "meeting-item";
          const link = getMeetingUrl(m.id);
          const joinLabel = m.require_join_approval ? "Waiting Room" : "Direct Join";
          const screenLabel = m.require_screen_approval ? "Screen Approval" : "Open Share";
          const recLabel = m.record_meeting ? " • Auto-Record" : "";
          item.innerHTML = `
            <div class="meeting-meta">
              <span class="meeting-title">${escapeHtml(m.title)}</span>
              <span class="meeting-sub">Code: <b>${escapeHtml(m.id)}</b> • ${escapeHtml(formatDateTime(m.scheduled_at))} • ${joinLabel} • ${screenLabel}${recLabel}</span>
            </div>
            <div class="row">
              <button class="secondary copy-m-btn">Copy Link</button>
              <button class="join-m-btn">Join</button>
              <button class="danger del-m-btn">Delete</button>
            </div>
          `;
          item.querySelector(".copy-m-btn").onclick = (e) => copyText(link, e.target);
          item.querySelector(".join-m-btn").onclick = () => enterMeetingScreen(m.id, m.title);
          item.querySelector(".del-m-btn").onclick = async () => {
            await fetch(`/api/meetings/${encodeURIComponent(m.id)}`, { method: "DELETE" });
            loadMeetings();
          };
          meetingList.appendChild(item);
        }
      } catch {
        meetingList.innerHTML = `<div class="meeting-sub">Failed to load meetings.</div>`;
      }
    }

    instantBtn.onclick = async () => {
      const res = await fetch("/api/meetings", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          title: `${getDisplayName()}'s Meeting`,
          host_name: getDisplayName(),
          scheduled_at: null,
          require_join_approval: instantJoinMode.value === "approval",
          require_screen_approval: instantApprovalChk.checked,
          record_meeting: instantRecordChk.checked
        })
      });
      const meeting = await res.json();
      if (meeting.host_key) localStorage.setItem(`vc_host_key_${meeting.id}`, meeting.host_key);
      history.pushState({}, "", `/room/${meeting.id}`);
      enterMeetingScreen(meeting.id, meeting.title);
    };

    schedBtn.onclick = async () => {
      const title = schedTitle.value.trim() || "Scheduled Meeting";
      const dtVal = schedTime.value;
      if (!dtVal) return alert("Please pick a date and time to schedule.");

      const res = await fetch("/api/meetings", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          title,
          host_name: getDisplayName(),
          scheduled_at: new Date(dtVal).toISOString(),
          require_join_approval: schedJoinMode.value === "approval",
          require_screen_approval: schedApprovalChk.checked,
          record_meeting: schedRecordChk.checked
        })
      });
      const meeting = await res.json();
      if (meeting.host_key) localStorage.setItem(`vc_host_key_${meeting.id}`, meeting.host_key);
      schedLinkBox.textContent = `Scheduled! Link: ${getMeetingUrl(meeting.id)}`;
      schedResult.classList.remove("hidden");
      schedTitle.value = "";
      if (isMainHostServer) loadMeetings();
    };

    refreshListBtn.onclick = loadMeetings;
    refreshRecBtn.onclick = loadRecordings;

    async function checkUrlForRoom() {
      const match = location.pathname.match(/^\/room\/([^/]+)$/);
      if (!match) {
        directRoomCard.classList.add("hidden");
        lobbyHomeSections.classList.remove("hidden");
        return;
      }

      lobbyHomeSections.classList.add("hidden");

      const roomId = decodeURIComponent(match[1]);
      const res = await fetch(`/api/meetings/${encodeURIComponent(roomId)}`);
      const meeting = await res.json();

      directRoomCard.classList.remove("hidden");
      directRoomTitle.textContent = meeting.title || `Room ${roomId}`;
      const joinTypeText = meeting.require_join_approval ? "Waiting Room Enabled" : "Direct Join";
      directRoomTime.textContent = meeting.scheduled_at
        ? `Scheduled for: ${formatDateTime(meeting.scheduled_at)} • ${joinTypeText}`
        : `Meeting Code: ${roomId} • ${joinTypeText}`;
      directRoomLink.textContent = getMeetingUrl(roomId);
      directJoinBtn.onclick = () => enterMeetingScreen(roomId, meeting.title);
      directCopyBtn.onclick = (e) => copyText(getMeetingUrl(roomId), e.target);
    }

    joinBtn.onclick = () => {
      const val = roomInput.value.trim();
      if (!val) return alert("Enter a meeting code or room name.");
      const match = val.match(/\/room\/([^/?#]+)/);
      const roomId = match ? match[1] : val;
      history.pushState({}, "", `/room/${encodeURIComponent(roomId)}`);
      enterMeetingScreen(roomId, `Room ${roomId}`);
    };

    // --- Chat & Reactions ---
    function refreshDmDropdown() {
      const prevSelected = dmTarget.value;
      dmTarget.innerHTML = "";
      const entries = Object.entries(peerNames);
      if (entries.length === 0) {
        const opt = document.createElement("option");
        opt.value = "";
        opt.textContent = "No other participants in room";
        dmTarget.appendChild(opt);
      } else {
        for (const [id, name] of entries) {
          const opt = document.createElement("option");
          opt.value = id;
          opt.textContent = `${name} (${id.slice(0, 4)})`;
          dmTarget.appendChild(opt);
        }
        if (prevSelected && peerNames[prevSelected]) dmTarget.value = prevSelected;
      }
      renderParticipantsList();
    }

    function updateUnreadBadges() {
      badgePublic.textContent = unreadPublic;
      badgePublic.classList.toggle("hidden", unreadPublic === 0);
      badgePrivate.textContent = unreadPrivate;
      badgePrivate.classList.toggle("hidden", unreadPrivate === 0);
      const total = unreadPublic + unreadPrivate;
      chatTotalBadge.textContent = total;
      chatTotalBadge.classList.toggle("hidden", total === 0 || !chatPanel.classList.contains("hidden"));
    }

    function switchChatTab(tab) {
      activeChatTab = tab;
      const isPublic = tab === "public";
      tabPublic.classList.toggle("active-tab", isPublic);
      tabPrivate.classList.toggle("active-tab", !isPublic);
      publicMessages.classList.toggle("hidden", !isPublic);
      privateMessages.classList.toggle("hidden", isPublic);
      dmBar.classList.toggle("hidden", isPublic);
      chatInput.placeholder = isPublic ? "Message everyone..." : "Send a private message...";
      if (isPublic) unreadPublic = 0;
      else unreadPrivate = 0;
      updateUnreadBadges();
    }

    tabPublic.onclick = () => switchChatTab("public");
    tabPrivate.onclick = () => switchChatTab("private");

    toggleChatBtn.onclick = () => {
      const opening = chatPanel.classList.contains("hidden");
      chatPanel.classList.toggle("hidden", !opening);
      if (opening) {
        participantsPanel.classList.add("hidden");
        controlsDock.classList.remove("more-open");
        if (activeChatTab === "public") unreadPublic = 0;
        else unreadPrivate = 0;
      }
      updateUnreadBadges();
    };

    function appendChatMessage({ container, senderName, text, isSelf, isDm = false, dmLabel = "", replyToId = null }) {
      const item = document.createElement("div");
      item.className = `msg-item${isSelf ? " self" : ""}${isDm ? " dm" : ""}`;
      const timeStr = new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
      const tagHtml = isDm ? `<span class="dm-tag">${escapeHtml(dmLabel)}</span>` : "";
      item.innerHTML = `
        <div class="msg-meta">${tagHtml}${escapeHtml(senderName)} • ${timeStr}</div>
        <div class="msg-bubble">${escapeHtml(text)}</div>
      `;
      if (isDm && !isSelf && replyToId && peerNames[replyToId]) {
        item.title = "Click to reply privately";
        item.onclick = () => { dmTarget.value = replyToId; chatInput.focus(); };
      }
      container.appendChild(item);
      container.scrollTop = container.scrollHeight;
    }

    function sendChatMessage() {
      const text = chatInput.value.trim();
      if (!text) return;
      if (activeChatTab === "public") {
        sendSignal({ type: "chat_public", text });
        appendChatMessage({
          container: publicMessages,
          senderName: `${getDisplayName()} (You)`,
          text,
          isSelf: true
        });
      } else {
        const targetId = dmTarget.value;
        if (!targetId || !peerNames[targetId]) {
          return alert("Select a participant to send a personal message.");
        }
        sendSignal({ type: "chat_private", target: targetId, text });
        appendChatMessage({
          container: privateMessages,
          senderName: "You",
          text,
          isSelf: true,
          isDm: true,
          dmLabel: `To ${peerNames[targetId]}`
        });
      }
      chatInput.value = "";
    }

    sendChatBtn.onclick = sendChatMessage;
    chatInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") sendChatMessage();
    });

    function showReaction(emoji, senderName, tileId) {
      const el = document.createElement("div");
      el.className = "floating-reaction";
      el.style.left = `${15 + Math.random() * 70}%`;
      el.innerHTML = `
        <span class="emoji">${escapeHtml(emoji)}</span>
        <span class="who">${escapeHtml(senderName)}</span>
      `;
      el.addEventListener("animationend", () => el.remove());
      reactionLayer.appendChild(el);

      const tile = document.getElementById(`tile-${tileId}`);
      if (tile) {
        let badge = tile.querySelector(".tile-reaction");
        if (!badge) {
          badge = document.createElement("div");
          badge.className = "tile-reaction";
          tile.appendChild(badge);
        }
        badge.textContent = emoji;
        clearTimeout(tileReactionTimers[tileId]);
        tileReactionTimers[tileId] = setTimeout(() => badge.remove(), 3000);
      }
    }

    document.querySelectorAll(".react-btn").forEach(btn => {
      btn.onclick = () => {
        const emoji = btn.dataset.emoji;
        sendSignal({ type: "reaction", emoji });
        showReaction(emoji, getDisplayName(), "local");
      };
    });

    // --- Video Grid, Avatars, Pinning & Pagination ---
    function ensureVideoTile(id, labelText, isMuted = false, avatarUrl = "") {
      let tile = document.getElementById(`tile-${id}`);
      const cleanName = labelText.replace(/\s*\(You\)$/, "");
      if (!tile) {
        tile = document.createElement("div");
        tile.id = `tile-${id}`;
        tile.className = "video-tile";

        const video = document.createElement("video");
        video.autoplay = true;
        video.playsInline = true;
        video.muted = isMuted;

        const placeholder = document.createElement("div");
        placeholder.className = "placeholder";

        const avatarEl = document.createElement("div");
        avatarEl.className = "avatar-circle large";
        renderAvatarCircle(avatarEl, avatarUrl, cleanName);

        const phText = document.createElement("div");
        phText.className = "ph-text";
        phText.textContent = "Camera Off";

        placeholder.appendChild(avatarEl);
        placeholder.appendChild(phText);

        const pinBtn = document.createElement("button");
        pinBtn.type = "button";
        pinBtn.className = "pin-btn";
        pinBtn.textContent = "📌 Pin";
        pinBtn.title = "Pin / Spotlight this participant";
        pinBtn.onclick = (e) => {
          e.stopPropagation();
          if (pinnedTileId === id) {
            pinnedTileId = null;
          } else {
            pinnedTileId = id;
            if (currentViewMode === "grid" || currentViewMode === "compact") {
              applyViewMode("speaker");
              return;
            }
          }
          updateFeaturedTile();
        };

        const label = document.createElement("div");
        label.className = "label";
        label.textContent = labelText;

        tile.appendChild(video);
        tile.appendChild(placeholder);
        tile.appendChild(pinBtn);
        tile.appendChild(label);
        videoGrid.appendChild(tile);

        if (!tileOrder.includes(id)) tileOrder.push(id);
        videoGrid.classList.toggle("single-tile", tileOrder.length <= 1);
        updateFeaturedTile();
        renderPagination();
      } else {
        tile.querySelector(".label").textContent = labelText;
        const avatarEl = tile.querySelector(".placeholder .avatar-circle");
        if (avatarEl) renderAvatarCircle(avatarEl, avatarUrl, cleanName);
      }
      return tile;
    }

    function setTilePlaceholder(id, showVideo, placeholderText = "Camera Off") {
      const tile = document.getElementById(`tile-${id}`);
      if (!tile) return;
      const placeholder = tile.querySelector(".placeholder");
      const phText = placeholder.querySelector(".ph-text");
      if (phText) phText.textContent = placeholderText;
      placeholder.classList.toggle("hidden", showVideo);
    }

    function removeVideoTile(id) {
      if (pinnedTileId === id) pinnedTileId = null;
      if (lastActiveSpeakerTileId === id) lastActiveSpeakerTileId = null;
      const tile = document.getElementById(`tile-${id}`);
      if (tile) tile.remove();
      tileOrder = tileOrder.filter(x => x !== id);
      videoGrid.classList.toggle("single-tile", tileOrder.length <= 1);
      updateFeaturedTile();
      renderPagination();
    }

    function renderPagination() {
      const pageSize = getPageSize();
      const totalPages = Math.max(1, Math.ceil(tileOrder.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;

      const start = (currentPage - 1) * pageSize;
      const end = start + pageSize;

      tileOrder.forEach((id, idx) => {
        const tile = document.getElementById(`tile-${id}`);
        if (tile) {
          const isFeatured = tile.classList.contains("featured-tile");
          tile.classList.toggle("page-hidden", !isFeatured && (idx < start || idx >= end));
        }
      });

      paginationBar.classList.toggle("hidden", totalPages <= 1);
      pageIndicator.textContent = `Page ${currentPage} / ${totalPages}`;
      prevPageBtn.disabled = currentPage <= 1;
      nextPageBtn.disabled = currentPage >= totalPages;
    }

    prevPageBtn.onclick = () => { if (currentPage > 1) { currentPage--; renderPagination(); } };
    nextPageBtn.onclick = () => {
      const totalPages = Math.max(1, Math.ceil(tileOrder.length / getPageSize()));
      if (currentPage < totalPages) { currentPage++; renderPagination(); }
    };

    // --- Hardware Detection ---
    async function checkDeviceExists(kind) {
      if (!navigator.mediaDevices?.getUserMedia) return false;
      if (navigator.mediaDevices.enumerateDevices) {
        const devices = await navigator.mediaDevices.enumerateDevices();
        return devices.some(d => d.kind === kind);
      }
      return true;
    }

    function handleMediaError(err, typeLabel) {
      if (err.name === "NotFoundError" || err.name === "DevicesNotFoundError" || err.name === "OverconstrainedError") {
        showNoticeBanner(`❌ No ${typeLabel} found on this device.`, "denied", 5000);
      } else if (err.name === "NotAllowedError" || err.name === "PermissionDeniedError") {
        showNoticeBanner(`⚠️ ${typeLabel} permission denied in browser settings.`, "denied", 5000);
      } else if (err.name === "NotReadableError" || err.name === "TrackStartError") {
        showNoticeBanner(`⚠️ ${typeLabel} is already in use by another app.`, "denied", 5000);
      } else {
        showNoticeBanner(`❌ Could not enable ${typeLabel}: ${err.message}`, "denied", 5000);
      }
    }

    // --- Microphone / Speaker Only ---
    async function enableMicrophone() {
      if (!(await checkDeviceExists("audioinput"))) {
        showNoticeBanner("❌ No microphone found on this device. Using Computer Speaker Only.", "denied");
        return false;
      }

      if (!lkRoom) {
        try {
          const testStream = await navigator.mediaDevices.getUserMedia({ audio: true });
          testStream.getTracks().forEach(t => t.stop());
          wantMicOnJoin = true;
          micEnabled = true;
          updateControlButtons();
          return true;
        } catch (err) {
          handleMediaError(err, "microphone");
          return false;
        }
      }

      try {
        await lkRoom.localParticipant.setMicrophoneEnabled(true);
        micEnabled = true;
        const localMicPub = lkRoom.localParticipant.getTrackPublication(LivekitClient.Track.Source.Microphone);
        if (localMicPub?.track?.mediaStreamTrack) {
          startLocalVoiceMonitor(localMicPub.track.mediaStreamTrack);
        }
        syncAllAudioToRecorder();
        if (captionsEnabled) startSpeechRecognition();
        updateControlButtons();
        return true;
      } catch (err) {
        handleMediaError(err, "microphone");
        micEnabled = false;
        updateControlButtons();
        return false;
      }
    }

    async function useSpeakerOnly() {
      wantMicOnJoin = false;
      micEnabled = false;
      stopLocalVoiceMonitor();
      stopSpeechRecognition();
      if (lkRoom) {
        await lkRoom.localParticipant.setMicrophoneEnabled(false);
      }
      updateControlButtons();
    }

    // --- Camera Enable / Disable ---
    async function enableCamera() {
      if (!(await checkDeviceExists("videoinput"))) {
        showNoticeBanner("❌ No camera found on this device.", "denied", 5000);
        return false;
      }

      if (!lkRoom) {
        try {
          const testStream = await navigator.mediaDevices.getUserMedia({ video: true });
          const tile = ensureVideoTile("local", `${getDisplayName()} (You)`, true, localAvatar);
          tile.querySelector("video").srcObject = testStream;
          setTilePlaceholder("local", true, "Camera Disabled");
          wantCamOnJoin = true;
          camEnabled = true;
          updateControlButtons();
          return true;
        } catch (err) {
          handleMediaError(err, "camera");
          return false;
        }
      }

      try {
        await lkRoom.localParticipant.setCameraEnabled(true);
        camEnabled = true;
        const pub = lkRoom.localParticipant.getTrackPublication(LivekitClient.Track.Source.Camera);
        const localVideo = ensureVideoTile("local", `${getDisplayName()} (You)`, true, localAvatar).querySelector("video");
        if (pub?.track) pub.track.attach(localVideo);
        setTilePlaceholder("local", true, "Camera Disabled");
        updateControlButtons();
        return true;
      } catch (err) {
        handleMediaError(err, "camera");
        camEnabled = false;
        updateControlButtons();
        return false;
      }
    }

    async function disableCamera() {
      wantCamOnJoin = false;
      camEnabled = false;
      const localTile = document.getElementById("tile-local");
      const localVideo = localTile?.querySelector("video");

      if (lkRoom) {
        const pub = lkRoom.localParticipant.getTrackPublication(LivekitClient.Track.Source.Camera);
        if (pub?.track && localVideo) pub.track.detach(localVideo);
        await lkRoom.localParticipant.setCameraEnabled(false);
      } else if (localVideo?.srcObject) {
        localVideo.srcObject.getTracks().forEach(t => t.stop());
        localVideo.srcObject = null;
      }

      setTilePlaceholder("local", false, "Camera Disabled");
      updateControlButtons();
    }

    // --- HD 1080p Screen Share ---
    function showRemoteScreenStage(presenterName, track, publication) {
      activeScreenPublication = publication || null;
      screenStage.classList.remove("hidden");
      screenStageVideo.classList.remove("hidden");
      screenFullscreenBtn.classList.remove("hidden");
      onFullscreenChange();
      screenStageLabel.textContent = `🖥️ ${presenterName} is presenting (1080p HD)`;
      track.attach(screenStageVideo);
      applyHighQualityScreenShare();
    }

    async function hideScreenStage() {
      activeScreenPublication = null;
      await exitScreenFullscreenIfActive();
      screenStage.classList.add("hidden");
      screenFullscreenBtn.classList.add("hidden");
      screenStageVideo.classList.remove("hidden");
      screenStageVideo.srcObject = null;
      if (localScreenMirrorVideo) {
        localScreenMirrorVideo.srcObject = null;
        localScreenMirrorVideo = null;
      }
    }

    async function launchDisplayMedia() {
      if (!lkRoom) return;
      const isSafari = /^((?!chrome|android).)*safari/i.test(navigator.userAgent);
      const hdEncoding = { maxBitrate: 6_000_000, maxFramerate: 30 };

      try {
        await lkRoom.localParticipant.setScreenShareEnabled(
          true,
          {
            audio: false,
            selfBrowserSurface: "exclude",
            contentHint: "detail",
            ...(isSafari ? {} : { resolution: LivekitClient.ScreenSharePresets.h1080fps30.resolution })
          },
          {
            screenShareEncoding: hdEncoding,
            simulcast: false,
            degradationPreference: "maintain-resolution"
          }
        );
      } catch {
        return;
      }

      isSharingScreen = true;
      screenShareApproved = false;
      screenRequestPending = false;
      sendSignal({ type: "screen_share_started" });

      const pub = lkRoom.localParticipant.getTrackPublication(LivekitClient.Track.Source.ScreenShare);
      if (pub?.track?.mediaStreamTrack) {
        localScreenMirrorVideo = document.createElement("video");
        localScreenMirrorVideo.muted = true;
        localScreenMirrorVideo.autoplay = true;
        localScreenMirrorVideo.playsInline = true;
        localScreenMirrorVideo.srcObject = new MediaStream([pub.track.mediaStreamTrack]);
        localScreenMirrorVideo.play().catch(() => {});
      }

      screenStage.classList.remove("hidden");
      screenStageVideo.classList.add("hidden");
      screenFullscreenBtn.classList.add("hidden");
      screenStageLabel.textContent = "🖥️ You are sharing your screen in 1080p HD";
      updateControlButtons();
    }

    async function stopScreenShare(notifyServer = true) {
      isSharingScreen = false;
      if (lkRoom) {
        try {
          await lkRoom.localParticipant.setScreenShareEnabled(false);
        } catch {}
      }
      if (notifyServer) {
        sendSignal({ type: "screen_share_stopped" });
      }
      await hideScreenStage();
      updateControlButtons();
    }

    // --- LiveKit SFU Connection ---
    async function connectLiveKit(url, token) {
      if (typeof LivekitClient === "undefined") {
        showNoticeBanner("❌ LiveKit client script failed to load. Check internet connection.", "denied", 0);
        return;
      }

      lkRoom = new LivekitClient.Room({
        adaptiveStream: true,
        dynacast: true,
        videoCaptureDefaults: {
          resolution: LivekitClient.VideoPresets.h720.resolution
        },
        publishDefaults: {
          screenShareEncoding: { maxBitrate: 6_000_000, maxFramerate: 30 },
          degradationPreference: "maintain-resolution"
        }
      });

      lkRoom.on(LivekitClient.RoomEvent.ActiveSpeakersChanged, (speakers) => {
        lkSpeakingIds.clear();
        for (const sp of speakers) {
          lkSpeakingIds.add(sp.identity === clientId ? "local" : sp.identity);
        }
        updateSpeakingUI();
      });

      lkRoom.on(LivekitClient.RoomEvent.TrackSubscribed, (track, publication, participant) => {
        const pid = participant.identity;
        const pName = participant.name || peerNames[pid] || `User ${pid}`;
        const pAvatar = peerAvatars[pid] || "";

        if (track.kind === LivekitClient.Track.Kind.Audio) {
          const audioEl = track.attach();
          audioEl.id = `audio-${track.sid}`;
          audioContainer.appendChild(audioEl);
          if (track.mediaStreamTrack) attachAudioTrackToRecorder(track.mediaStreamTrack);
        } else if (publication.source === LivekitClient.Track.Source.ScreenShare) {
          showRemoteScreenStage(pName, track, publication);
        } else if (publication.source === LivekitClient.Track.Source.Camera) {
          const tile = ensureVideoTile(pid, pName, false, pAvatar);
          track.attach(tile.querySelector("video"));
          setTilePlaceholder(pid, !publication.isMuted, "Camera Off");
        }
        renderParticipantsList();
      });

      lkRoom.on(LivekitClient.RoomEvent.TrackUnsubscribed, async (track, publication, participant) => {
        if (track.kind === LivekitClient.Track.Kind.Audio) {
          track.detach().forEach(el => el.remove());
        } else if (publication.source === LivekitClient.Track.Source.ScreenShare) {
          track.detach(screenStageVideo);
          await hideScreenStage();
        } else if (publication.source === LivekitClient.Track.Source.Camera) {
          const tile = document.getElementById(`tile-${participant.identity}`);
          if (tile) {
            track.detach(tile.querySelector("video"));
            setTilePlaceholder(participant.identity, false, "Camera Off");
          }
        }
        renderParticipantsList();
      });

      lkRoom.on(LivekitClient.RoomEvent.TrackPublished, () => renderParticipantsList());
      lkRoom.on(LivekitClient.RoomEvent.TrackUnpublished, () => renderParticipantsList());

      lkRoom.on(LivekitClient.RoomEvent.TrackMuted, (publication, participant) => {
        if (publication.source === LivekitClient.Track.Source.Camera && participant.identity !== clientId) {
          setTilePlaceholder(participant.identity, false, "Camera Off");
        }
        renderParticipantsList();
      });

      lkRoom.on(LivekitClient.RoomEvent.TrackUnmuted, (publication, participant) => {
        if (publication.source === LivekitClient.Track.Source.Camera && participant.identity !== clientId) {
          setTilePlaceholder(participant.identity, true, "Camera Off");
        }
        renderParticipantsList();
      });

      lkRoom.on(LivekitClient.RoomEvent.LocalTrackPublished, (publication) => {
        if (publication.source === LivekitClient.Track.Source.Camera && publication.track) {
          const localVideo = ensureVideoTile("local", `${getDisplayName()} (You)`, true, localAvatar).querySelector("video");
          publication.track.attach(localVideo);
          setTilePlaceholder("local", true, "Camera Disabled");
        } else if (publication.source === LivekitClient.Track.Source.Microphone && publication.track?.mediaStreamTrack) {
          startLocalVoiceMonitor(publication.track.mediaStreamTrack);
          attachAudioTrackToRecorder(publication.track.mediaStreamTrack);
        } else if (publication.source === LivekitClient.Track.Source.ScreenShare && publication.track?.mediaStreamTrack) {
          publication.track.mediaStreamTrack.contentHint = "detail";
        }
        renderParticipantsList();
      });

      lkRoom.on(LivekitClient.RoomEvent.LocalTrackUnpublished, async (publication) => {
        if (publication.source === LivekitClient.Track.Source.ScreenShare && isSharingScreen) {
          isSharingScreen = false;
          sendSignal({ type: "screen_share_stopped" });
          await hideScreenStage();
          updateControlButtons();
        }
        renderParticipantsList();
      });

      try {
        await lkRoom.connect(url, token);
      } catch (err) {
        console.error("LiveKit connection error:", err);
        const msg = err?.message || String(err);
        let hint = "Ensure livekit-server is running with livekit.yaml.";
        if (msg.toLowerCase().includes("pc connection")) {
          hint = "WebRTC ICE failed. Check livekit.yaml (enable_loopback_candidate: true) and Brave WebRTC IP policy.";
        } else if (msg.toLowerCase().includes("signal")) {
          hint = "Cannot reach port 7880. Make sure livekit-server is running.";
        }
        showNoticeBanner(`❌ LiveKit error (${url}): ${msg}. ${hint}`, "denied", 0);
        return;
      }

      const localVideo = document.getElementById("tile-local")?.querySelector("video");
      if (localVideo?.srcObject) {
        localVideo.srcObject.getTracks().forEach(t => t.stop());
        localVideo.srcObject = null;
      }

      if (wantMicOnJoin) await enableMicrophone();
      if (wantCamOnJoin) await enableCamera();

      for (const participant of lkRoom.remoteParticipants.values()) {
        const pid = participant.identity;
        const pName = participant.name || peerNames[pid] || `User ${pid}`;
        const pAvatar = peerAvatars[pid] || "";
        ensureVideoTile(pid, pName, false, pAvatar);
        for (const pub of participant.trackPublications.values()) {
          if (pub.isSubscribed && pub.track) {
            if (pub.track.kind === LivekitClient.Track.Kind.Audio) {
              const el = pub.track.attach();
              el.id = `audio-${pub.track.sid}`;
              audioContainer.appendChild(el);
            } else if (pub.source === LivekitClient.Track.Source.ScreenShare) {
              showRemoteScreenStage(pName, pub.track, pub);
            } else if (pub.source === LivekitClient.Track.Source.Camera) {
              const tile = ensureVideoTile(pid, pName, false, pAvatar);
              pub.track.attach(tile.querySelector("video"));
              setTilePlaceholder(pid, !pub.isMuted, "Camera Off");
            }
          }
        }
      }
      renderParticipantsList();

      if (isHost && autoRecordOnJoin && !isRecordingLocally) {
        setTimeout(() => {
          if (hasJoinedRoom && isHost && !isRecordingLocally) startHostRecording();
        }, 400);
      }
    }

    // --- Controls ---
    function updateControlButtons() {
      useMicBtn.classList.toggle("selected-mic", micEnabled);
      useSpeakerBtn.classList.toggle("selected-speaker", !micEnabled);

      camBtn.disabled = false;
      camBtn.innerHTML = camEnabled
        ? '📷 <span class="hide-sm">Disable </span>Cam<span class="hide-sm">era</span>'
        : '📷 <span class="hide-sm">Enable </span>Cam<span class="hide-sm">era</span>';
      camBtn.classList.toggle("active", camEnabled);
      camBtn.classList.toggle("secondary", !camEnabled);

      modalCamBtn.textContent = camEnabled ? "📷 Disable Camera" : "📷 Enable Camera";
      modalCamBtn.classList.toggle("active", camEnabled);
      modalCamBtn.classList.toggle("secondary", !camEnabled);

      ccBtn.textContent = captionsEnabled ? "💬 CC: On" : "💬 CC: Off";
      ccBtn.classList.toggle("active", captionsEnabled);
      ccBtn.classList.toggle("secondary", !captionsEnabled);

      hostBadge.classList.toggle("hidden", !isHost);
      hostRecordBtn.classList.toggle("hidden", !isHost);
      if (isHost) {
        hostRecordBtn.textContent = isRecordingLocally ? "⏹ Stop Recording" : "🔴 Record";
        hostRecordBtn.classList.toggle("danger", isRecordingLocally);
        hostRecordBtn.classList.toggle("secondary", !isRecordingLocally);
      }

      hostJoinPolicyBtn.classList.toggle("hidden", !isHost);
      if (isHost) {
        hostJoinPolicyBtn.textContent = requireJoinApproval
          ? "Entry: Owner Approval"
          : "Entry: Direct Join";
        hostJoinPolicyBtn.classList.toggle("warning", requireJoinApproval);
        hostJoinPolicyBtn.classList.toggle("active", !requireJoinApproval);
      }

      hostPolicyBtn.classList.toggle("hidden", !isHost);
      if (isHost) {
        hostPolicyBtn.textContent = requireScreenApproval
          ? "Screen: Approval Req."
          : "Screen: Anyone";
        hostPolicyBtn.classList.toggle("warning", requireScreenApproval);
        hostPolicyBtn.classList.toggle("active", !requireScreenApproval);
      }

      const someoneElseSharing = Boolean(activeSharer && activeSharer !== clientId);
      hostStopShareBtn.classList.toggle("hidden", !(isHost && someoneElseSharing));

      screenBtn.classList.remove("active", "warning", "hidden");
      if (!navigator.mediaDevices?.getDisplayMedia) {
        screenBtn.classList.add("hidden");
      } else if (isSharingScreen) {
        screenBtn.disabled = false;
        screenBtn.textContent = "Stop Sharing";
        screenBtn.classList.add("active");
      } else if (someoneElseSharing) {
        screenBtn.disabled = true;
        screenBtn.textContent = `🔒 ${activeSharerName || "Someone"} is sharing`;
        const approvedBanner = document.getElementById("approved-banner");
        if (approvedBanner) approvedBanner.remove();
      } else if (screenRequestPending) {
        screenBtn.disabled = true;
        screenBtn.textContent = "Waiting for Host...";
      } else if (requireScreenApproval && !isHost && !screenShareApproved) {
        screenBtn.disabled = false;
        screenBtn.textContent = "Request Screen Share";
        screenBtn.classList.add("warning");
      } else if (screenShareApproved) {
        screenBtn.disabled = false;
        screenBtn.textContent = "Approved — Share Screen";
        screenBtn.classList.add("active");
      } else {
        screenBtn.disabled = false;
        screenBtn.textContent = "Share Screen";
      }

      updateWhiteboardUI();
      renderParticipantsList();
    }

    hostRecordBtn.onclick = async () => {
      if (!isHost) return;
      controlsDock.classList.remove("more-open");
      if (isRecordingLocally) await stopHostRecording(true);
      else await startHostRecording();
    };

    hostJoinPolicyBtn.onclick = () => {
      if (isHost) sendSignal({ type: "set_join_policy", require_approval: !requireJoinApproval });
    };

    useMicBtn.onclick = async () => { await enableMicrophone(); };
    useSpeakerBtn.onclick = async () => {
      await useSpeakerOnly();
      showNoticeBanner("🔊 Switched to Computer Speaker Only (microphone off).", "", 2500);
    };

    camBtn.onclick = async () => {
      if (camEnabled) await disableCamera();
      else await enableCamera();
    };

    modalCamBtn.onclick = async () => {
      if (camEnabled) await disableCamera();
      else await enableCamera();
    };

    modalMicBtn.onclick = async () => {
      await enableMicrophone();
      joinOptionsOverlay.classList.add("hidden");
      connectToRoomSocket();
    };

    modalSpeakerBtn.onclick = async () => {
      await useSpeakerOnly();
      joinOptionsOverlay.classList.add("hidden");
      connectToRoomSocket();
    };

    cancelWaitingBtn.onclick = () => { leaveBtn.click(); };

    hostPolicyBtn.onclick = () => {
      if (isHost) sendSignal({ type: "set_screen_policy", require_approval: !requireScreenApproval });
    };

    hostStopShareBtn.onclick = () => {
      if (isHost) sendSignal({ type: "host_stop_share" });
      controlsDock.classList.remove("more-open");
    };

    screenBtn.onclick = async () => {
      controlsDock.classList.remove("more-open");
      if (isSharingScreen) {
        await stopScreenShare(true);
        return;
      }
      if (activeSharer && activeSharer !== clientId) {
        showNoticeBanner(`${activeSharerName || "Another user"} is currently sharing.`, "denied");
        return;
      }
      if (requireScreenApproval && !isHost && !screenShareApproved) {
        screenRequestPending = true;
        updateControlButtons();
        sendSignal({ type: "screen_share_request" });
        showNoticeBanner("Screen share request sent to the host...", "", 3000);
        return;
      }
      const approvedBanner = document.getElementById("approved-banner");
      if (approvedBanner) approvedBanner.remove();
      await launchDisplayMedia();
    };

    function sendSignal(payload) {
      if (ws?.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(payload));
      }
    }

    function enterMeetingScreen(roomId, title) {
      currentRoomId = roomId;
      currentMeetingTitle = title || `Room ${roomId}`;
      wantMicOnJoin = false;
      wantCamOnJoin = false;
      micEnabled = false;
      camEnabled = false;
      hasJoinedRoom = false;
      waitingUsers = {};
      waitingCount = 0;
      currentHostId = null;
      pinnedTileId = null;
      lastActiveSpeakerTileId = null;
      lkSpeakingIds.clear();
      wbOpen = false;
      wbLocked = false;
      wbHistory = [];
      redrawWhiteboard();
      tileOrder = ["local"];
      currentPage = 1;

      renderAvatarPreviews();
      lobbyView.classList.add("hidden");
      callView.classList.remove("hidden");
      callTitleText.textContent = currentMeetingTitle;
      callSub.textContent = `Code: ${roomId} • Link: ${getMeetingUrl(roomId)}`;
      joinModalTitle.textContent = `Join "${currentMeetingTitle}"`;

      refreshDmDropdown();
      ensureVideoTile("local", `${getDisplayName()} (You)`, true, localAvatar);
      setTilePlaceholder("local", false, "Camera Disabled");
      applyViewMode(currentViewMode);
      updateControlButtons();
      joinOptionsOverlay.classList.remove("hidden");
    }

    function connectToRoomSocket() {
      if (hasJoinedRoom) return;
      hasJoinedRoom = true;

      const hostKey = localStorage.getItem(`vc_host_key_${currentRoomId}`) || "";
      const protocol = location.protocol === "https:" ? "wss:" : "ws:";
      const wsUrl = `${protocol}//${location.host}/ws/${encodeURIComponent(currentRoomId)}/${clientId}?name=${encodeURIComponent(getDisplayName())}&host_key=${encodeURIComponent(hostKey)}`;
      ws = new WebSocket(wsUrl);

      ws.onopen = () => {
        sendSignal({
          type: "join_room",
          name: getDisplayName(),
          avatar: localAvatar
        });
      };

      ws.onmessage = async (event) => {
        const msg = JSON.parse(event.data);

        if (msg.type === "room_full") {
          alert(msg.reason || "Room is full.");
          leaveBtn.click();
          return;
        }

        if (msg.type === "waiting_room") {
          waitingRoomOverlay.classList.remove("hidden");
          waitingRoomStatus.textContent = msg.host_present
            ? "Waiting for the meeting owner to let you in..."
            : "Waiting for the meeting owner to join and let you in...";
          return;
        }

        if (msg.type === "join_denied") {
          const reason = msg.reason || "The meeting owner declined your request to join.";
          await leaveBtn.onclick();
          showNoticeBanner(`❌ ${reason}`, "denied", 5000);
          alert(reason);
          return;
        }

        if (msg.type === "waiting_user_joined") {
          if (isHost) {
            waitingUsers[msg.user.id] = msg.user;
            waitingCount = Object.keys(waitingUsers).length;
            playWaitingKnockSound();
            showWaitingUserBanner(msg.user);
            renderParticipantsList();
          }
        } else if (msg.type === "waiting_list") {
          if (isHost && msg.users?.length) {
            msg.users.forEach(u => {
              waitingUsers[u.id] = u;
              showWaitingUserBanner(u);
            });
            waitingCount = Object.keys(waitingUsers).length;
            playWaitingKnockSound();
            renderParticipantsList();
          }
        } else if (msg.type === "waiting_user_left") {
          delete waitingUsers[msg.sender];
          waitingCount = Object.keys(waitingUsers).length;
          const card = document.getElementById(`wait-${msg.sender}`);
          if (card) card.remove();
          renderParticipantsList();

        } else if (msg.type === "waiting_count_update") {
          waitingCount = msg.waiting_count || 0;
          renderParticipantsList();

        } else if (msg.type === "all_users") {
          waitingRoomOverlay.classList.add("hidden");

          isHost = Boolean(msg.is_host);
          currentHostId = msg.host_id || null;
          requireScreenApproval = Boolean(msg.require_screen_approval);
          requireJoinApproval = Boolean(msg.require_join_approval);
          autoRecordOnJoin = Boolean(msg.auto_record);
          activeSharer = msg.active_sharer || null;
          activeSharerName = msg.active_sharer_name || null;
          waitingCount = msg.waiting_count || 0;
          maxUsersInRoom = msg.max_users || 100;

          wbOpen = Boolean(msg.whiteboard_open);
          wbLocked = Boolean(msg.whiteboard_locked);
          wbHistory = Array.isArray(msg.whiteboard_strokes) ? msg.whiteboard_strokes : [];
          redrawWhiteboard();

          updateRecordingUI(Boolean(msg.is_recording), msg.recording_by || "Host");
          if (msg.is_recording) {
            showNoticeBanner("🔴 Notice: This meeting is currently being recorded by the host.", "denied", 6000);
          }

          for (const user of msg.users) {
            peerNames[user.id] = user.name;
            peerAvatars[user.id] = user.avatar || "";
            ensureVideoTile(user.id, user.name, false, user.avatar || "");
          }
          refreshDmDropdown();
          updateControlButtons();

          if (msg.livekit_url && msg.livekit_token) {
            await connectLiveKit(msg.livekit_url, msg.livekit_token);
          }

        } else if (msg.type === "room_policy") {
          isHost = msg.host_id === clientId;
          currentHostId = msg.host_id || null;
          requireScreenApproval = Boolean(msg.require_screen_approval);
          requireJoinApproval = Boolean(msg.require_join_approval);
          activeSharer = msg.active_sharer || null;
          activeSharerName = msg.active_sharer_name || null;
          waitingCount = msg.waiting_count ?? waitingCount;
          if (typeof msg.whiteboard_open === "boolean") wbOpen = msg.whiteboard_open;
          if (typeof msg.whiteboard_locked === "boolean") wbLocked = msg.whiteboard_locked;
          updateRecordingUI(Boolean(msg.is_recording), msg.recording_by || "Host");
          if (!requireScreenApproval) {
            screenRequestPending = false;
            screenShareApproved = false;
          }
          updateControlButtons();

        } else if (msg.type === "wb_toggle") {
          wbOpen = Boolean(msg.open);
          updateWhiteboardUI();
          if (wbOpen) {
            redrawWhiteboard();
            showNoticeBanner(`🎨 ${msg.by || "Someone"} opened the whiteboard.`, "", 2500);
          }

        } else if (msg.type === "wb_lock") {
          wbLocked = Boolean(msg.locked);
          updateWhiteboardUI();

        } else if (msg.type === "wb_live") {
          drawSingleStroke(wbCtx, {
            tool: msg.tool,
            color: msg.color,
            size: msg.size,
            points: [[msg.x1, msg.y1], [msg.x2, msg.y2]]
          });

        } else if (msg.type === "wb_commit") {
          if (msg.stroke) {
            wbHistory.push(msg.stroke);
            redrawWhiteboard();
          }

        } else if (msg.type === "wb_undo") {
          wbHistory = wbHistory.filter(s => s.id !== msg.stroke_id);
          redrawWhiteboard();

        } else if (msg.type === "wb_clear") {
          wbHistory = [];
          redrawWhiteboard();
          showNoticeBanner(`🗑️ ${msg.by || "Someone"} cleared the whiteboard.`, "", 2200);

        } else if (msg.type === "recording_status") {
          updateRecordingUI(Boolean(msg.is_recording), msg.by || "Host");
          if (msg.is_recording) {
            showNoticeBanner(`🔴 Notice: ${msg.by || "The host"} started recording this meeting.`, "denied", 6000);
          } else {
            showNoticeBanner("⏹ The host stopped recording this meeting.", "", 4000);
          }

        } else if (msg.type === "caption") {
          displayCaption(msg.sender_name || "Participant", msg.text);

        } else if (msg.type === "screen_share_request") {
          if (isHost) showHostRequestBanner(msg.sender, msg.sender_name || "Participant");

        } else if (msg.type === "screen_share_approved") {
          screenRequestPending = false;
          screenShareApproved = true;
          updateControlButtons();
          showApprovedBanner();

        } else if (msg.type === "screen_share_denied") {
          screenRequestPending = false;
          screenShareApproved = false;
          updateControlButtons();
          showNoticeBanner(msg.reason || "Screen share request was denied.", "denied", 4500);

        } else if (msg.type === "screen_share_revoked") {
          if (isSharingScreen) await stopScreenShare(false);
          screenRequestPending = false;
          screenShareApproved = false;
          updateControlButtons();
          showNoticeBanner(msg.reason || "Screen share stopped.", "denied", 4500);

        } else if (msg.type === "user_joined") {
          peerNames[msg.user.id] = msg.user.name;
          peerAvatars[msg.user.id] = msg.user.avatar || "";
          ensureVideoTile(msg.user.id, msg.user.name, false, msg.user.avatar || "");
          refreshDmDropdown();
          playJoinSound();
          showNoticeBanner(`👋 ${msg.user.name} joined the meeting.`, "", 2800);

        } else if (msg.type === "reaction") {
          showReaction(msg.emoji, msg.sender_name || "Participant", msg.sender);

        } else if (msg.type === "chat_public") {
          appendChatMessage({
            container: publicMessages,
            senderName: msg.sender_name || "Participant",
            text: msg.text,
            isSelf: false
          });
          if (chatPanel.classList.contains("hidden") || activeChatTab !== "public") {
            unreadPublic++;
            updateUnreadBadges();
          }

        } else if (msg.type === "chat_private") {
          appendChatMessage({
            container: privateMessages,
            senderName: msg.sender_name || "Participant",
            text: msg.text,
            isSelf: false,
            isDm: true,
            dmLabel: "Private",
            replyToId: msg.sender
          });
          if (chatPanel.classList.contains("hidden") || activeChatTab !== "private") {
            unreadPrivate++;
            updateUnreadBadges();
          }

        } else if (msg.type === "user_left") {
          delete peerNames[msg.sender];
          delete peerAvatars[msg.sender];
          lkSpeakingIds.delete(msg.sender);
          const reqBanner = document.getElementById(`req-${msg.sender}`);
          if (reqBanner) reqBanner.remove();
          removeVideoTile(msg.sender);
          refreshDmDropdown();
        }
      };

      updateControlButtons();
    }

    copyCallLinkBtn.onclick = () => copyText(getMeetingUrl(currentRoomId), copyCallLinkBtn);

    leaveBtn.onclick = async () => {
      if (isRecordingLocally) {
        await stopHostRecording(true);
      }
      stopLocalVoiceMonitor();
      stopSpeechRecognition();
      captionsEnabled = false;
      clearTimeout(captionClearTimer);
      captionBar.classList.add("hidden");

      await exitScreenFullscreenIfActive();
      if (document.fullscreenElement === whiteboardStage || document.webkitFullscreenElement === whiteboardStage) {
        try {
          if (document.exitFullscreen) await document.exitFullscreen();
          else if (document.webkitExitFullscreen) await document.webkitExitFullscreen();
        } catch {}
      }

      if (ws) ws.close();
      ws = null;

      if (lkRoom) {
        await lkRoom.disconnect();
        lkRoom = null;
      }

      const localVideo = document.getElementById("tile-local")?.querySelector("video");
      if (localVideo?.srcObject) {
        localVideo.srcObject.getTracks().forEach(t => t.stop());
        localVideo.srcObject = null;
      }

      peerNames = {};
      peerAvatars = {};
      waitingUsers = {};
      waitingCount = 0;
      currentHostId = null;
      pinnedTileId = null;
      lastActiveSpeakerTileId = null;
      lkSpeakingIds.clear();
      wbOpen = false;
      wbLocked = false;
      wbHistory = [];
      redrawWhiteboard();
      tileOrder = ["local"];
      currentPage = 1;
      wantMicOnJoin = false;
      wantCamOnJoin = false;
      micEnabled = false;
      camEnabled = false;
      isSharingScreen = false;
      hasJoinedRoom = false;
      isHost = false;
      autoRecordOnJoin = false;
      isMeetingRecording = false;
      requireJoinApproval = false;
      activeSharer = null;
      activeSharerName = null;
      screenRequestPending = false;
      screenShareApproved = false;

      audioContainer.innerHTML = "";
      await hideScreenStage();
      whiteboardStage.classList.add("hidden");
      videoGrid.innerHTML = "";
      reactionLayer.innerHTML = "";
      bannerContainer.innerHTML = "";
      publicMessages.innerHTML = "";
      privateMessages.innerHTML = "";
      participantSearch.value = "";
      controlsDock.classList.remove("more-open");
      participantsPanel.classList.add("hidden");
      if (window.innerWidth <= 768) chatPanel.classList.add("hidden");
      joinOptionsOverlay.classList.add("hidden");
      waitingRoomOverlay.classList.add("hidden");
      recordingPill.classList.add("hidden");
      unreadPublic = 0;
      unreadPrivate = 0;
      updateUnreadBadges();
      switchChatTab("public");

      callView.classList.add("hidden");
      lobbyView.classList.remove("hidden");

      checkUrlForRoom();
      if (isMainHostServer) {
        loadMeetings();
        loadRecordings();
      }
    };

    window.addEventListener("popstate", checkUrlForRoom);

    checkOwnerAccess();
    checkUrlForRoom();
  </script>
</body>
</html>"""


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
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
                require_join_approval INTEGER NOT NULL DEFAULT 0
            )
        """)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(meetings)").fetchall()}
        if "host_key" not in cols:
            conn.execute("ALTER TABLE meetings ADD COLUMN host_key TEXT")
        if "require_screen_approval" not in cols:
            conn.execute(
                "ALTER TABLE meetings ADD COLUMN require_screen_approval INTEGER NOT NULL DEFAULT 1"
            )
        if "record_meeting" not in cols:
            conn.execute(
                "ALTER TABLE meetings ADD COLUMN record_meeting INTEGER NOT NULL DEFAULT 0"
            )
        if "require_join_approval" not in cols:
            conn.execute(
                "ALTER TABLE meetings ADD COLUMN require_join_approval INTEGER NOT NULL DEFAULT 0"
            )


init_db()


def is_main_host_request(request: Request) -> bool:
    """True only when accessed directly on the main host server (not external/proxied users)."""
    client_ip = request.client.host if request.client else ""
    if client_ip not in ("127.0.0.1", "::1"):
        return False
    # Block external users coming through reverse proxies / tunnels (ngrok, Cloudflare, nginx, etc.)
    for hdr in ("x-forwarded-for", "x-real-ip", "forwarded", "cf-connecting-ip", "x-forwarded-host"):
        if request.headers.get(hdr):
            return False
    host_hdr = (request.headers.get("host") or "").split(":")[0].lower()
    return host_hdr in ("localhost", "127.0.0.1", "[::1]")


def require_main_host(request: Request):
    if not is_main_host_request(request):
        raise HTTPException(
            status_code=403,
            detail="Only the owner on the main host server can access this.",
        )


def generate_meeting_code() -> str:
    alphabet = string.ascii_lowercase + string.digits
    p1 = "".join(secrets.choice(alphabet) for _ in range(3))
    p2 = "".join(secrets.choice(alphabet) for _ in range(4))
    p3 = "".join(secrets.choice(alphabet) for _ in range(3))
    return f"{p1}-{p2}-{p3}"


def get_meeting_row(meeting_id: str) -> dict | None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM meetings WHERE id = ?", (meeting_id,)).fetchone()
    return dict(row) if row else None


def sanitize_avatar(avatar: str | None) -> str:
    if not isinstance(avatar, str):
        return ""
    if avatar.startswith("data:image/") and len(avatar) <= 60_000:
        return avatar
    return ""


def mint_livekit_token(room_id: str, client_id: str, name: str, is_host: bool) -> str:
    return (
        lk_api.AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
        .with_identity(client_id)
        .with_name(name)
        .with_grants(
            lk_api.VideoGrants(
                room_join=True,
                room=room_id,
                can_publish=True,
                can_subscribe=True,
                can_publish_data=True,
                room_admin=is_host,
            )
        )
        .to_jwt()
    )


class MeetingCreate(BaseModel):
    title: str = "Instant Meeting"
    host_name: str = "Host"
    scheduled_at: str | None = None
    require_screen_approval: bool = True
    record_meeting: bool = False
    require_join_approval: bool = False


@app.get("/")
@app.get("/room/{room_id}")
async def serve_app(room_id: str | None = None):
    return HTMLResponse(HTML_CONTENT)


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

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """INSERT INTO meetings
               (id, title, host_name, scheduled_at, created_at, host_key,
                require_screen_approval, record_meeting, require_join_approval)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                meeting_id,
                title,
                host_name,
                payload.scheduled_at,
                created_at,
                host_key,
                int(payload.require_screen_approval),
                int(payload.record_meeting),
                int(payload.require_join_approval),
            ),
        )

    return {
        "id": meeting_id,
        "title": title,
        "host_name": host_name,
        "scheduled_at": payload.scheduled_at,
        "created_at": created_at,
        "host_key": host_key,
        "require_screen_approval": payload.require_screen_approval,
        "record_meeting": payload.record_meeting,
        "require_join_approval": payload.require_join_approval,
        "link": f"/room/{meeting_id}",
    }


@app.get("/api/meetings")
async def list_meetings(request: Request):
    require_main_host(request)
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT id, title, host_name, scheduled_at, created_at,
                      require_screen_approval, record_meeting, require_join_approval
               FROM meetings
               ORDER BY COALESCE(scheduled_at, created_at) ASC"""
        ).fetchall()
    result = []
    for row in rows:
        d = dict(row)
        d["require_screen_approval"] = bool(d["require_screen_approval"])
        d["record_meeting"] = bool(d["record_meeting"])
        d["require_join_approval"] = bool(d["require_join_approval"])
        result.append(d)
    return result


@app.get("/api/meetings/{meeting_id}")
async def get_meeting(meeting_id: str):
    row = get_meeting_row(meeting_id)
    if not row:
        return {
            "id": meeting_id,
            "title": f"Room {meeting_id}",
            "host_name": None,
            "scheduled_at": None,
            "require_screen_approval": True,
            "record_meeting": False,
            "require_join_approval": False,
        }
    return {
        "id": row["id"],
        "title": row["title"],
        "host_name": row["host_name"],
        "scheduled_at": row["scheduled_at"],
        "require_screen_approval": bool(row["require_screen_approval"]),
        "record_meeting": bool(row["record_meeting"]),
        "require_join_approval": bool(row["require_join_approval"]),
    }


@app.delete("/api/meetings/{meeting_id}")
async def delete_meeting(meeting_id: str, request: Request):
    require_main_host(request)
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("DELETE FROM meetings WHERE id = ?", (meeting_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Meeting not found")
    return {"deleted": True}


@app.post("/api/recordings/{room_id}")
async def save_recording(room_id: str, request: Request):
    # Allow saving if on main host server OR authenticated with the meeting's host_key
    allowed = is_main_host_request(request)
    if not allowed:
        req_host_key = request.headers.get("x-host-key", "")
        row = get_meeting_row(room_id)
        if row and row.get("host_key") and req_host_key:
            allowed = secrets.compare_digest(req_host_key, row["host_key"])
    if not allowed:
        raise HTTPException(status_code=403, detail="Only the meeting owner can save recordings.")

    data = await request.body()
    if not data:
        raise HTTPException(status_code=400, detail="Empty recording")
    safe_room = "".join(c for c in room_id if c.isalnum() or c in "-_") or "meeting"
    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"meeting-{safe_room}-{stamp}.webm"
    file_path = RECORDINGS_DIR / filename
    file_path.write_bytes(data)
    return {
        "saved": True,
        "filename": filename,
        "path": str(file_path),
        "url": f"/recordings/{filename}",
    }


@app.get("/api/recordings")
async def list_recordings(request: Request):
    require_main_host(request)
    files = sorted(RECORDINGS_DIR.glob("*.webm"), key=lambda p: p.stat().st_mtime, reverse=True)
    return [
        {
            "filename": f.name,
            "size_mb": round(f.stat().st_size / (1024 * 1024), 2),
            "url": f"/recordings/{f.name}",
        }
        for f in files
    ]


def _parse_range_header(range_header: str, file_size: int) -> tuple[int, int]:
    try:
        unit, range_spec = range_header.strip().split("=", 1)
        if unit.lower() != "bytes":
            raise ValueError
        start_str, end_str = range_spec.split("-", 1)
        if start_str == "" and end_str == "":
            raise ValueError
        if start_str == "":
            start = max(0, file_size - int(end_str))
            end = file_size - 1
        else:
            start = int(start_str)
            end = int(end_str) if end_str != "" else file_size - 1
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
        return StreamingResponse(
            _iter_file_range(file_path, start, end),
            status_code=206,
            headers=headers,
            media_type="video/webm",
        )

    headers["Content-Length"] = str(file_size)
    return StreamingResponse(
        _iter_file_range(file_path, 0, file_size - 1),
        status_code=200,
        headers=headers,
        media_type="video/webm",
    )


async def broadcast_room(room_id: str, payload: dict, exclude_id: str | None = None):
    text = json.dumps(payload)
    for uid, peer_info in list(rooms.get(room_id, {}).items()):
        if uid != exclude_id:
            try:
                await peer_info["ws"].send_text(text)
            except Exception:
                pass


async def broadcast_waiting_count(room_id: str):
    count = len(waiting_rooms.get(room_id, {}))
    await broadcast_room(room_id, {"type": "waiting_count_update", "waiting_count": count})


def build_policy_payload(room_id: str) -> dict:
    meta = room_meta[room_id]
    return {
        "type": "room_policy",
        "host_id": meta["host_id"],
        "require_screen_approval": meta["require_screen_approval"],
        "require_join_approval": meta["require_join_approval"],
        "active_sharer": meta["active_sharer"],
        "active_sharer_name": meta["active_sharer_name"],
        "is_recording": meta["is_recording"],
        "recording_by": meta["recording_by"],
        "waiting_count": len(waiting_rooms.get(room_id, {})),
        "whiteboard_open": meta["whiteboard_open"],
        "whiteboard_locked": meta["whiteboard_locked"],
    }


async def admit_user_to_room(room_id: str, uid: str, ws: WebSocket, name: str, avatar: str, is_host: bool):
    meta = room_meta[room_id]
    existing_users = [
        {"id": eid, "name": info["name"], "avatar": info["avatar"]}
        for eid, info in rooms[room_id].items()
    ]
    rooms[room_id][uid] = {"ws": ws, "name": name, "avatar": avatar}
    lk_token = mint_livekit_token(room_id, uid, name, is_host)

    await ws.send_text(json.dumps({
        "type": "all_users",
        "users": existing_users,
        "is_host": is_host,
        "host_id": meta["host_id"],
        "require_screen_approval": meta["require_screen_approval"],
        "require_join_approval": meta["require_join_approval"],
        "auto_record": meta["auto_record"],
        "is_recording": meta["is_recording"],
        "recording_by": meta["recording_by"],
        "active_sharer": meta["active_sharer"],
        "active_sharer_name": meta["active_sharer_name"],
        "waiting_count": len(waiting_rooms.get(room_id, {})),
        "whiteboard_open": meta["whiteboard_open"],
        "whiteboard_locked": meta["whiteboard_locked"],
        "whiteboard_strokes": meta["whiteboard_strokes"],
        "livekit_url": LIVEKIT_URL,
        "livekit_token": lk_token,
        "max_users": MAX_USERS_PER_ROOM,
    }))

    await broadcast_room(
        room_id,
        {"type": "user_joined", "user": {"id": uid, "name": name, "avatar": avatar}},
        exclude_id=uid,
    )

    if is_host:
        await broadcast_room(room_id, build_policy_payload(room_id), exclude_id=uid)
        if waiting_rooms.get(room_id):
            waiting_list = [
                {"id": wid, "name": winfo["name"], "avatar": winfo["avatar"]}
                for wid, winfo in waiting_rooms[room_id].items()
            ]
            await ws.send_text(json.dumps({
                "type": "waiting_list",
                "users": waiting_list,
            }))


@app.websocket("/ws/{room_id}/{client_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    room_id: str,
    client_id: str,
    name: str = "Guest",
    host_key: str = "",
):
    await websocket.accept()

    meeting_row = get_meeting_row(room_id)
    if room_id not in rooms:
        rooms[room_id] = {}
        waiting_rooms[room_id] = {}
        room_meta[room_id] = {
            "host_id": None,
            "require_screen_approval": bool(meeting_row["require_screen_approval"]) if meeting_row else True,
            "require_join_approval": bool(meeting_row["require_join_approval"]) if meeting_row else False,
            "auto_record": bool(meeting_row["record_meeting"]) if meeting_row else False,
            "active_sharer": None,
            "active_sharer_name": None,
            "approved_sharers": set(),
            "is_recording": False,
            "recording_by": None,
            "whiteboard_open": False,
            "whiteboard_locked": False,
            "whiteboard_strokes": [],
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

            if msg_type == "join_room" and not joined_or_waiting:
                joined_or_waiting = True
                user_name = (message.get("name") or name or "Guest").strip()[:60]
                user_avatar = sanitize_avatar(message.get("avatar"))

                if len(rooms[room_id]) >= MAX_USERS_PER_ROOM:
                    await websocket.send_text(json.dumps({
                        "type": "room_full",
                        "reason": f"Room is full (maximum {MAX_USERS_PER_ROOM} participants)."
                    }))
                    await websocket.close()
                    return

                if meeting_row and meeting_row.get("host_key"):
                    is_host = bool(host_key and secrets.compare_digest(host_key, meeting_row["host_key"]))
                else:
                    is_host = meta["host_id"] is None

                if is_host:
                    meta["host_id"] = client_id

                if is_host or not meta["require_join_approval"]:
                    await admit_user_to_room(room_id, client_id, websocket, user_name, user_avatar, is_host)
                else:
                    waiting_rooms[room_id][client_id] = {
                        "ws": websocket,
                        "name": user_name,
                        "avatar": user_avatar,
                    }
                    await websocket.send_text(json.dumps({
                        "type": "waiting_room",
                        "title": meeting_row["title"] if meeting_row else f"Room {room_id}",
                        "host_present": bool(meta["host_id"] and meta["host_id"] in rooms[room_id]),
                    }))
                    host_id = meta["host_id"]
                    if host_id and host_id in rooms[room_id]:
                        await rooms[room_id][host_id]["ws"].send_text(json.dumps({
                            "type": "waiting_user_joined",
                            "user": {"id": client_id, "name": user_name, "avatar": user_avatar},
                        }))
                    await broadcast_waiting_count(room_id)

            elif msg_type == "join_decision":
                if client_id == meta["host_id"] and target in waiting_rooms.get(room_id, {}):
                    approved = bool(message.get("approved"))
                    w_info = waiting_rooms[room_id].pop(target)
                    if approved:
                        if len(rooms[room_id]) >= MAX_USERS_PER_ROOM:
                            await w_info["ws"].send_text(json.dumps({
                                "type": "join_denied",
                                "reason": f"Room is full (maximum {MAX_USERS_PER_ROOM} participants)."
                            }))
                            await w_info["ws"].close()
                        else:
                            await admit_user_to_room(
                                room_id, target, w_info["ws"], w_info["name"], w_info["avatar"], False
                            )
                    else:
                        await w_info["ws"].send_text(json.dumps({
                            "type": "join_denied",
                            "reason": "The meeting owner declined your request to join."
                        }))
                        await w_info["ws"].close()
                    await broadcast_waiting_count(room_id)

            elif msg_type == "admit_all":
                if client_id == meta["host_id"] and waiting_rooms.get(room_id):
                    for wid, w_info in list(waiting_rooms[room_id].items()):
                        waiting_rooms[room_id].pop(wid, None)
                        if len(rooms[room_id]) < MAX_USERS_PER_ROOM:
                            await admit_user_to_room(
                                room_id, wid, w_info["ws"], w_info["name"], w_info["avatar"], False
                            )
                        else:
                            await w_info["ws"].send_text(json.dumps({
                                "type": "join_denied",
                                "reason": f"Room is full (maximum {MAX_USERS_PER_ROOM} participants)."
                            }))
                            await w_info["ws"].close()
                    await broadcast_waiting_count(room_id)

            elif msg_type == "set_join_policy":
                if client_id == meta["host_id"]:
                    new_val = bool(message.get("require_approval", False))
                    meta["require_join_approval"] = new_val
                    if meeting_row:
                        with sqlite3.connect(DB_PATH) as conn:
                            conn.execute(
                                "UPDATE meetings SET require_join_approval = ? WHERE id = ?",
                                (int(new_val), room_id),
                            )
                    if not new_val and waiting_rooms.get(room_id):
                        for wid, w_info in list(waiting_rooms[room_id].items()):
                            waiting_rooms[room_id].pop(wid, None)
                            if len(rooms[room_id]) < MAX_USERS_PER_ROOM:
                                await admit_user_to_room(
                                    room_id, wid, w_info["ws"], w_info["name"], w_info["avatar"], False
                                )
                            else:
                                await w_info["ws"].send_text(json.dumps({
                                    "type": "join_denied",
                                    "reason": f"Room is full (maximum {MAX_USERS_PER_ROOM})."
                                }))
                    await broadcast_room(room_id, build_policy_payload(room_id))

            elif client_id not in rooms.get(room_id, {}):
                continue

            elif msg_type == "wb_toggle":
                meta["whiteboard_open"] = bool(message.get("open", False))
                await broadcast_room(room_id, {
                    "type": "wb_toggle",
                    "open": meta["whiteboard_open"],
                    "by": user_name,
                }, exclude_id=client_id)

            elif msg_type == "wb_lock":
                if client_id == meta["host_id"]:
                    meta["whiteboard_locked"] = bool(message.get("locked", False))
                    await broadcast_room(room_id, {
                        "type": "wb_lock",
                        "locked": meta["whiteboard_locked"],
                    })

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
                        await broadcast_room(room_id, {
                            "type": "wb_commit",
                            "stroke": stroke,
                        }, exclude_id=client_id)

            elif msg_type == "wb_undo":
                stroke_id = message.get("stroke_id")
                if stroke_id:
                    before = len(meta["whiteboard_strokes"])
                    meta["whiteboard_strokes"] = [
                        s for s in meta["whiteboard_strokes"]
                        if not (
                            s.get("id") == stroke_id
                            and (s.get("sender") == client_id or client_id == meta["host_id"])
                        )
                    ]
                    if len(meta["whiteboard_strokes"]) != before:
                        await broadcast_room(room_id, {
                            "type": "wb_undo",
                            "stroke_id": stroke_id,
                        }, exclude_id=client_id)

            elif msg_type == "wb_clear":
                if not meta["whiteboard_locked"] or client_id == meta["host_id"]:
                    meta["whiteboard_strokes"].clear()
                    await broadcast_room(room_id, {
                        "type": "wb_clear",
                        "by": user_name,
                    }, exclude_id=client_id)

            elif msg_type in ("chat_public", "reaction", "caption"):
                message["sender"] = client_id
                message["sender_name"] = user_name
                await broadcast_room(room_id, message, exclude_id=client_id)

            elif msg_type == "chat_private":
                if target and target in rooms.get(room_id, {}):
                    message["sender"] = client_id
                    message["sender_name"] = user_name
                    await rooms[room_id][target]["ws"].send_text(json.dumps(message))

            elif msg_type == "recording_started":
                if client_id == meta["host_id"]:
                    meta["is_recording"] = True
                    meta["recording_by"] = user_name
                    await broadcast_room(room_id, {
                        "type": "recording_status",
                        "is_recording": True,
                        "by": user_name,
                    })

            elif msg_type == "recording_stopped":
                if client_id == meta["host_id"]:
                    meta["is_recording"] = False
                    meta["recording_by"] = None
                    await broadcast_room(room_id, {
                        "type": "recording_status",
                        "is_recording": False,
                        "by": user_name,
                    })

            elif msg_type == "set_screen_policy":
                if client_id == meta["host_id"]:
                    new_val = bool(message.get("require_approval", True))
                    meta["require_screen_approval"] = new_val
                    if not new_val:
                        meta["approved_sharers"].clear()
                    if meeting_row:
                        with sqlite3.connect(DB_PATH) as conn:
                            conn.execute(
                                "UPDATE meetings SET require_screen_approval = ? WHERE id = ?",
                                (int(new_val), room_id),
                            )
                    await broadcast_room(room_id, build_policy_payload(room_id))

            elif msg_type == "screen_share_request":
                if meta["active_sharer"] and meta["active_sharer"] != client_id:
                    await websocket.send_text(json.dumps({
                        "type": "screen_share_denied",
                        "reason": f"{meta['active_sharer_name']} is already sharing their screen."
                    }))
                elif not meta["require_screen_approval"] or client_id == meta["host_id"]:
                    meta["approved_sharers"].add(client_id)
                    await websocket.send_text(json.dumps({"type": "screen_share_approved"}))
                else:
                    host_id = meta["host_id"]
                    if not host_id or host_id not in rooms.get(room_id, {}):
                        await websocket.send_text(json.dumps({
                            "type": "screen_share_denied",
                            "reason": "The meeting host is not in the room yet."
                        }))
                    else:
                        await rooms[room_id][host_id]["ws"].send_text(json.dumps({
                            "type": "screen_share_request",
                            "sender": client_id,
                            "sender_name": user_name,
                        }))

            elif msg_type == "screen_share_decision":
                if client_id == meta["host_id"] and target in rooms.get(room_id, {}):
                    approved = bool(message.get("approved"))
                    if approved:
                        if meta["active_sharer"] and meta["active_sharer"] != target:
                            await rooms[room_id][target]["ws"].send_text(json.dumps({
                                "type": "screen_share_denied",
                                "reason": f"{meta['active_sharer_name']} is currently sharing."
                            }))
                        else:
                            meta["approved_sharers"].add(target)
                            await rooms[room_id][target]["ws"].send_text(json.dumps({
                                "type": "screen_share_approved"
                            }))
                    else:
                        meta["approved_sharers"].discard(target)
                        await rooms[room_id][target]["ws"].send_text(json.dumps({
                            "type": "screen_share_denied",
                            "reason": "The meeting host declined your screen share request."
                        }))

            elif msg_type == "screen_share_started":
                if meta["active_sharer"] and meta["active_sharer"] != client_id:
                    await websocket.send_text(json.dumps({
                        "type": "screen_share_revoked",
                        "reason": f"{meta['active_sharer_name']} is already sharing their screen."
                    }))
                elif (
                    meta["require_screen_approval"]
                    and client_id != meta["host_id"]
                    and client_id not in meta["approved_sharers"]
                ):
                    await websocket.send_text(json.dumps({
                        "type": "screen_share_revoked",
                        "reason": "Host approval is required to share your screen."
                    }))
                else:
                    meta["approved_sharers"].discard(client_id)
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
                        await rooms[room_id][sharer_id]["ws"].send_text(json.dumps({
                            "type": "screen_share_revoked",
                            "reason": "The meeting host stopped your screen share."
                        }))
                    await broadcast_room(room_id, build_policy_payload(room_id))

    except WebSocketDisconnect:
        if room_id in waiting_rooms and client_id in waiting_rooms[room_id]:
            del waiting_rooms[room_id][client_id]
            host_id = meta["host_id"]
            if host_id and host_id in rooms.get(room_id, {}):
                try:
                    await rooms[room_id][host_id]["ws"].send_text(json.dumps({
                        "type": "waiting_user_left",
                        "sender": client_id,
                    }))
                except Exception:
                    pass
            await broadcast_waiting_count(room_id)

        if room_id in rooms and client_id in rooms[room_id]:
            del rooms[room_id][client_id]
            meta["approved_sharers"].discard(client_id)

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
                if meta["is_ad_hoc"] and rooms[room_id]:
                    meta["host_id"] = next(iter(rooms[room_id].keys()))
                else:
                    meta["host_id"] = None
                policy_changed = True

            await broadcast_room(room_id, {"type": "user_left", "sender": client_id})
            if recording_stopped and rooms[room_id]:
                await broadcast_room(room_id, {"type": "recording_status", "is_recording": False})
            if policy_changed and rooms[room_id]:
                await broadcast_room(room_id, build_policy_payload(room_id))

        if room_id in rooms and not rooms[room_id] and not waiting_rooms.get(room_id):
            rooms.pop(room_id, None)
            waiting_rooms.pop(room_id, None)
            room_meta.pop(room_id, None)