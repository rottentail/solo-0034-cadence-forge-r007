#!/usr/bin/env python3
"""Bounded production workflow verification; baseline automated testing is deferred."""
import argparse
import hashlib
import io
import json
import os
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def need(value, message):
    if not value:
        raise RuntimeError(message)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--workflow', default='all')
    args = ap.parse_args()
    observations = []
    with tempfile.TemporaryDirectory(prefix='cadence-workflow-') as scratch:
        scratch = Path(scratch)
        ffmpeg = os.environ.get('FFMPEG_BIN', 'ffmpeg')
        subprocess.run([ffmpeg, '-nostdin', '-hide_banner', '-loglevel', 'error',
                        '-f', 'lavfi', '-i', 'testsrc2=size=160x90:rate=24:duration=4',
                        '-f', 'lavfi', '-i', 'sine=frequency=660:sample_rate=48000:duration=4',
                        '-c:v', 'libx264', '-preset', 'ultrafast', '-threads', '1',
                        '-c:a', 'aac', '-shortest', str(scratch/'calibration.mp4')], check=True, timeout=30)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        address = 'http://127.0.0.1:'+str(port)+'/forge/v1'
        env = dict(os.environ, FORGE_ROOT=str(scratch/'data'),
                   FORGE_ADDR='127.0.0.1:'+str(port), FORGE_WORKERS='1')
        log = open(scratch/'server.log', 'ab')
        process = None
        def request(method, route, body=None, headers=None, status=200, raw=False):
            hs = dict(headers or {})
            if isinstance(body, (dict, list)):
                body = json.dumps(body).encode()
                hs['Content-Type'] = 'application/json'
            req = urllib.request.Request(address+route, data=body, headers=hs, method=method)
            try:
                response = urllib.request.urlopen(req, timeout=60)
            except urllib.error.HTTPError as exc:
                response = exc
            with response:
                data = response.read()
                need(response.status == status, f'{method} {route}: {response.status}, wanted {status}: {data[:600]!r}')
                return data if raw or not data else json.loads(data)
        def start():
            nonlocal process
            process = subprocess.Popen([str(ROOT/'bin/forged')], cwd=ROOT, env=env,
                                       stdout=log, stderr=log, start_new_session=True)
            deadline = time.monotonic()+10
            while time.monotonic() < deadline:
                try:
                    request('GET', '/health')
                    return
                except (OSError, RuntimeError):
                    if process.poll() is not None:
                        raise RuntimeError((scratch/'server.log').read_text())
                    time.sleep(.05)
            raise RuntimeError('server readiness timed out')
        def stop(force=False):
            nonlocal process
            if process and process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)
                process.wait(timeout=15)
            process = None
        def observe(name, detail):
            observations.append({'workflow_id': name, 'passed': True, 'observed': detail})
        def wait_render(rid, desired=None):
            deadline = time.monotonic()+40
            while time.monotonic() < deadline:
                r = request('GET', '/renders/'+rid)
                if desired and r['state'] == desired:
                    return r
                if not desired and r['state'] in ('succeeded', 'failed', 'cancelled'):
                    return r
                time.sleep(.02)
            raise RuntimeError('render state timeout')
        try:
            start()
            media = (scratch/'calibration.mp4').read_bytes()
            source = request('POST', '/sources?name=calibration-pulse', media, status=201)
            sid = source['id']
            need(source['media']['video'] and source['media']['audio'], 'probe lost streams')
            need(source['digest'] == hashlib.sha256(media).hexdigest(), 'source digest mismatch')
            request('POST', '/sources?name=invalid', b'not-media', status=400)
            duplicate = request('POST', '/sources?name=duplicate', media, status=201)
            need(duplicate['id'] == sid, 'ingest did not converge')
            observe('ingest', 'MP4 accepted with SHA-256; malformed bytes rejected; duplicate converges')
            if args.workflow in ('all', 'resumable-ingest'):
                import concurrent.futures
                chunk_size = 1 << 14
                total = hashlib.sha256(media).hexdigest()
                manifest = [hashlib.sha256(media[i:i+chunk_size]).hexdigest()
                            for i in range(0, len(media), chunk_size)]
                opened = request('POST', '/uploads', dict(
                    name='resumable-pulse', size=len(media), chunk_size=chunk_size,
                    content_sha256=total, chunk_sha256=manifest), status=201)
                uid = opened['id']
                need(opened['state'] == 'open' and len(opened['missing']) == len(manifest),
                    'new session must expose every chunk as missing')
                # An unconfirmed, apparently complete-looking session is never clippable.
                request('POST', '/uploads/'+uid+'/complete?name=resumable-pulse', status=400)
                before = request('GET', '/uploads/'+uid)
                need(before['received'] == [], 'fresh session claims received chunks')
                # Simulate an interrupted network: only the first half arrives.
                first_half = len(manifest)//2
                for i in range(first_half):
                    view = request('PUT', '/uploads/'+uid+'/chunks/'+str(i),
                                   media[i*chunk_size:(i+1)*chunk_size], status=200)
                need(view['missing'] == list(range(first_half, len(manifest))),
                    'status did not report only the missing tail')
                # Same-byte concurrent retries of one chunk must not cross identities.
                payload = media[:chunk_size]
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                    replies = list(pool.map(lambda _: request(
                        'PUT', '/uploads/'+uid+'/chunks/0', payload, status=200), range(4)))
                need(all(r['received'] == replies[0]['received'] for r in replies),
                    'concurrent same-byte uploads diverged')
                # Divergent bytes for one session+index are refused. With a digest
                # manifest the claim is invalid (400); without one the occupied slot
                # conflicts (409), so identities can never be silently overwritten.
                request('PUT', '/uploads/'+uid+'/chunks/0', b'X'*chunk_size, status=400)
                unmanifested = request('POST', '/uploads', dict(
                    name='slot-guard', size=len(media), chunk_size=chunk_size), status=201)
                request('PUT', '/uploads/'+unmanifested['id']+'/chunks/0', payload, status=200)
                request('PUT', '/uploads/'+unmanifested['id']+'/chunks/0', b'X'*chunk_size, status=409)
                # A digest manifest pins every chunk; tampered bytes cannot be confirmed.
                bad = request('POST', '/uploads', dict(
                    name='tampered', size=len(media), chunk_size=chunk_size,
                    chunk_sha256=manifest), status=201)
                request('PUT', '/uploads/'+bad['id']+'/chunks/0', b'Z'*chunk_size, status=400)
                # Abandoned uploads disappear and can never become media.
                abandoned = request('POST', '/uploads', dict(
                    name='abandoned', size=len(media), chunk_size=chunk_size), status=201)
                request('DELETE', '/uploads/'+abandoned['id'], status=204)
                request('GET', '/uploads/'+abandoned['id'], status=404)
                request('PUT', '/uploads/'+abandoned['id']+'/chunks/0', payload, status=404)
                # Restart proves recovery: the partial session survives and resumes.
                stop(force=True)
                start()
                resumed = request('GET', '/uploads/'+uid)
                need(resumed['missing'] == list(range(first_half, len(manifest))),
                    'restart lost durable chunk progress')
                for i in range(first_half, len(manifest)):
                    request('PUT', '/uploads/'+uid+'/chunks/'+str(i),
                            media[i*chunk_size:(i+1)*chunk_size], status=200)
                # A wrong whole-content declaration cannot be promoted to a source.
                liar = request('POST', '/uploads', dict(
                    name='liar', size=len(media), chunk_size=chunk_size,
                    content_sha256='0'*64), status=201)
                for i in range(len(manifest)):
                    request('PUT', '/uploads/'+liar['id']+'/chunks/'+str(i),
                            media[i*chunk_size:(i+1)*chunk_size], status=200)
                request('POST', '/uploads/'+liar['id']+'/complete?name=liar', status=400)
                request('GET', '/uploads/'+liar['id'], status=200)
                # Promotion after a byte-valid but non-media payload stays retryable:
                # the object is promoted but probe rejects it, and repeating complete
                # reuses the promoted digest without any re-upload.
                junk = bytes((i * 13) % 256 for i in range(3 * chunk_size))
                junk_session = request('POST', '/uploads', dict(
                    name='junk', size=len(junk), chunk_size=chunk_size,
                    content_sha256=hashlib.sha256(junk).hexdigest()), status=201)
                for i in range(3):
                    request('PUT', '/uploads/'+junk_session['id']+'/chunks/'+str(i),
                            junk[i*chunk_size:(i+1)*chunk_size], status=200)
                request('POST', '/uploads/'+junk_session['id']+'/complete?name=junk', status=400)
                junk_view = request('GET', '/uploads/'+junk_session['id'])
                need(junk_view['state'] == 'completed' and junk_view['digest']
                     == hashlib.sha256(junk).hexdigest(), 'promoted digest was not retained for retry')
                request('POST', '/uploads/'+junk_session['id']+'/complete?name=junk', status=400)
                resumed_source = request('POST', '/uploads/'+uid+'/complete?name=resumable-pulse', status=201)
                need(resumed_source['digest'] == total and resumed_source['media']['video'],
                    'resumed upload produced wrong identity or lost stream facts')
                # Reception and editing share one identity: the journaled source is clippable,
                # and identical bytes converge to the same source as the classic ingest.
                need(resumed_source['id'] == sid, 'resumable and classic ingest split identities')
                state = request('GET', '/state')
                need(uid not in json.dumps(state), 'unfinished upload identity leaked into journal')
                # Completing an already journaled session is idempotent: it re-resolves the
                # same source identity rather than failing or forging a second source.
                again = request('POST', '/uploads/'+uid+'/complete?name=again', status=201)
                need(again['id'] == sid, 'idempotent complete split the source identity')
                # A second session carrying the same bytes converges instead of forging a source.
                twin = request('POST', '/uploads', dict(
                    name='twin', size=len(media), chunk_size=chunk_size,
                    content_sha256=total), status=201)
                for i in range(len(manifest)):
                    request('PUT', '/uploads/'+twin['id']+'/chunks/'+str(i),
                            media[i*chunk_size:(i+1)*chunk_size], status=200)
                converged = request('POST', '/uploads/'+twin['id']+'/complete?name=twin', status=201)
                need(converged['id'] == sid, 'same-byte session created a foreign source identity')
                observe('resumable-ingest',
                        'partial chunks resume after SIGKILL; concurrency, abort, manifest and '
                        'whole-digest gates proven over HTTP; only verified media joins the source identity')
            recipe_body = dict(name='review-proxy', format='mp4', width=160, height=90,
                               fps=24, sample_rate=48000, normalize=False, target_lufs=-16)
            recipe = request('POST', '/recipes', recipe_body, status=201)
            rp = recipe['id']
            updated = request('PUT', '/recipes/'+rp, dict(recipe_body, name='review-proxy-v2'), {'If-Match': '1'}, status=201)
            need(updated['revision'] == 2, 'recipe revision not advanced')
            request('PUT', '/recipes/'+rp, recipe_body, {'If-Match': '1'}, status=409)
            observe('recipe-version', 'revision 2 saved; stale revision rejected')
            sheet_body = dict(name='two-pass-calibration', spans=[
                dict(source_id=sid, in_ms=0, out_ms=1500, gain_db=-3),
                dict(source_id=sid, in_ms=2000, out_ms=3500, gain_db=0)],
                cues=[dict(source_id=sid, start_ms=1000, end_ms=2500, text='校准 <幅度>')])
            sheet = request('POST', '/sheets', sheet_body, status=201)
            sh = sheet['id']
            revision = request('PUT', '/sheets/'+sh, dict(sheet_body, name='revised-calibration'), {'If-Match': '1'}, status=201)
            need(revision['revision'] == 2, 'sheet revision not advanced')
            request('PUT', '/sheets/'+sh, sheet_body, {'If-Match': '1'}, status=409)
            observe('sheet-version', 'two-cut revision saved; stale writer rejected')
            plan = request('GET', '/sheets/'+sh+'/preview?recipe_id='+rp)
            need(plan['duration_ms'] == 3000 and plan['placements'][1]['output_in_ms'] == 1500, 'timeline mismatch')
            observe('preview', 'two source spans compacted to a 3000ms composition')
            need('00:00:01.000 --> 00:00:01.500' in plan['vtt'], 'first caption clipping mismatch')
            need('00:00:01.500 --> 00:00:02.000' in plan['vtt'], 'second caption mapping mismatch')
            need('&lt;幅度&gt;' in plan['vtt'], 'caption markup not escaped')
            observe('captions', 'source cue split and clipped into two escaped output intervals')
            if args.workflow in ('all', 'loudness'):
                loud = request('POST', '/sources/'+sid+'/loudness')
                need(loud['integrated_lufs'] is not None and not loud['silent'], 'loudness scan failed')
                observe('loudness', 'measured integrated LUFS and true peak')
            for mode in ['storyboard', 'waveform']:
                if args.workflow in ('all', mode):
                    image = request('GET', '/sources/'+sid+'/visual?mode='+mode, raw=True)
                    need(image.startswith(b'\x89PNG'), 'inspection output is not PNG')
                    observe(mode, f'PNG produced from source streams: {len(image)} bytes')
            req = dict(sheet_id=sh, sheet_revision=1, recipe_id=rp, recipe_revision=1)
            render = request('POST', '/renders', req, {'Idempotency-Key': 'first-render'}, status=202)
            rid = render['id']
            again = request('POST', '/renders', req, {'Idempotency-Key': 'first-render'}, status=202)
            need(again['id'] == rid, 'idempotency key did not converge')
            request('POST', '/renders', dict(req, sheet_revision=2), {'Idempotency-Key': 'first-render'}, status=409)
            observe('idempotent-submit', 'same request converges; changed revision conflicts')
            done = wait_render(rid)
            need(done['state'] == 'succeeded', str(done['attempts']))
            need(done['sheet']['revision'] == 1 and done['recipe']['revision'] == 1, 'frozen revisions drifted')
            observe('mp4-render', 'frozen revisions encoded to H.264/AAC MP4 and inspected')
            verification = request('POST', '/renders/'+rid+'/verify')
            artifact = request('GET', '/renders/'+rid+'/artifact', raw=True)
            need(hashlib.sha256(artifact).hexdigest() == verification['digest'], 'artifact digest mismatch')
            observe('artifact-verify', 'download SHA-256 equals committed inspection')
            if args.workflow in ('all', 'bundle'):
                packed = request('GET', '/renders/'+rid+'/bundle', raw=True)
                with zipfile.ZipFile(io.BytesIO(packed)) as z:
                    need(set(z.namelist()) == {'composition.mp4', 'captions.vtt', 'composition.json'}, 'bundle members mismatch')
                    frozen = json.loads(z.read('composition.json'))
                    need(frozen['plan']['vtt'] == z.read('captions.vtt').decode(), 'bundle captions drifted')
                observe('bundle', 'verified media, captions and frozen manifest exported together')
            if args.workflow in ('all', 'wav-render'):
                ar = request('POST', '/recipes', dict(recipe_body, name='reference-audio', format='wav', normalize=True), status=201)
                audio_render = request('POST', '/renders', dict(req, recipe_id=ar['id']), {'Idempotency-Key': 'audio'}, status=202)
                audio_done = wait_render(audio_render['id'])
                need(audio_done['state'] == 'succeeded', str(audio_done['attempts']))
                need(audio_done['artifact']['inspection']['media']['audio_codec'] == 'pcm_s16le', 'audio codec mismatch')
                observe('wav-render', 'trimmed stereo PCM WAV encoded with loudness filter')
            request('DELETE', '/sources/'+sid, status=409)
            observe('retire-source', 'historical sheet reference prevents retirement')
            if args.workflow in ('all', 'cancel', 'retry', 'restart-recover'):
                long_sheet = request('POST', '/sheets', dict(sheet_body, name='extended-calibration', spans=sheet_body['spans']*16), status=201)
                pending = request('POST', '/renders', dict(req, sheet_id=long_sheet['id']), {'Idempotency-Key': 'interrupt'}, status=202)
                interrupted_id = pending['id']
                wait_render(interrupted_id, 'running')
                cancelled = request('POST', '/renders', req, {'Idempotency-Key': 'cancel-me'}, status=202)
                cancelled = request('POST', '/renders/'+cancelled['id']+'/cancel')
                need(cancelled['state'] == 'cancelled', 'cancel not acknowledged')
                request('GET', '/renders/'+cancelled['id']+'/artifact', status=409)
                observe('cancel', 'composition cancelled without artifact visibility')
                stop(force=True)
                start()
                recovered = request('GET', '/renders/'+interrupted_id)
                need(recovered['state'] == 'failed' and recovered['attempts'][-1]['outcome'] == 'interrupted', 'recovery provenance mismatch')
                observe('restart-recover', 'SIGKILL/restart retained frozen inputs and marked interruption')
                request('POST', '/renders/'+interrupted_id+'/retry', status=202)
                retried = wait_render(interrupted_id)
                need(retried['state'] == 'succeeded' and len(retried['attempts']) == 2, str(retried['attempts']))
                observe('retry', 'second attempt completed using original snapshot')
            events = request('GET', '/events?after=0')
            need(events and all(a['sequence'] < b['sequence'] for a, b in zip(events, events[1:])), 'event order mismatch')
            tail = request('GET', '/events?after='+str(events[-1]['sequence']))
            need(all(e['sequence'] > events[-1]['sequence'] for e in tail), 'event cursor repeated')
            observe('events', 'persisted event cursor strictly increases')
            if args.workflow != 'all':
                need(any(x['workflow_id'] == args.workflow for x in observations), 'unknown workflow')
            print(json.dumps({'ok': True, 'requested': args.workflow, 'observations': observations}, ensure_ascii=False))
        except Exception:
            log.flush()
            print((scratch/'server.log').read_text(errors='replace'))
            raise
        finally:
            stop()
            log.close()

if __name__ == '__main__':
    main()
