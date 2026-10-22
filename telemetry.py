"""Windows process counters without third-party runtime dependencies."""
import ctypes
from ctypes import wintypes
import os


class ProcessEntry(ctypes.Structure):
    _fields_ = [('dwSize', wintypes.DWORD), ('cntUsage', wintypes.DWORD),
                ('pid', wintypes.DWORD), ('heap', ctypes.c_size_t),
                ('module', wintypes.DWORD), ('threads', wintypes.DWORD),
                ('parent', wintypes.DWORD), ('priority', wintypes.LONG),
                ('flags', wintypes.DWORD), ('exe', wintypes.WCHAR * 260)]


class Memory(ctypes.Structure):
    _fields_ = [('cb', wintypes.DWORD), ('faults', wintypes.DWORD)] + [
        (name, ctypes.c_size_t) for name in (
            'peak_working_set', 'working_set', 'peak_paged', 'paged',
            'peak_nonpaged', 'nonpaged', 'pagefile', 'peak_pagefile', 'private_bytes')]


def process_metrics():
    if os.name != 'nt':
        return {'gateway': {'pid': os.getpid()}}
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    psapi = ctypes.WinDLL('psapi', use_last_error=True)
    kernel.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.Process32FirstW.argtypes = kernel.Process32NextW.argtypes = [
        wintypes.HANDLE, ctypes.POINTER(ProcessEntry)]
    psapi.GetProcessMemoryInfo.argtypes = [
        wintypes.HANDLE, ctypes.POINTER(Memory), wintypes.DWORD]
    kernel.GetProcessHandleCount.argtypes = [
        wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    snapshot = kernel.CreateToolhelp32Snapshot(2, 0)
    pids = [('gateway', os.getpid())]
    if snapshot and snapshot != ctypes.c_void_p(-1).value:
        entry = ProcessEntry()
        entry.dwSize = ctypes.sizeof(entry)
        ok = kernel.Process32FirstW(snapshot, ctypes.byref(entry))
        while ok:
            if entry.exe.lower() in ('info.exe', 'quik.exe'):
                pids.append((entry.exe, entry.pid))
            ok = kernel.Process32NextW(snapshot, ctypes.byref(entry))
        kernel.CloseHandle(snapshot)
    result = {}
    for name, pid in pids:
        handle = kernel.OpenProcess(0x410, False, pid)
        if not handle:
            result[f'{name}:{pid}'] = {'pid': pid, 'unavailable': True}
            continue
        try:
            memory = Memory()
            memory.cb = ctypes.sizeof(memory)
            count = wintypes.DWORD()
            if not psapi.GetProcessMemoryInfo(handle, ctypes.byref(memory), memory.cb):
                result[f'{name}:{pid}'] = {'pid': pid, 'unavailable': True}
                continue
            kernel.GetProcessHandleCount(handle, ctypes.byref(count))
            result[f'{name}:{pid}'] = dict(
                pid=pid, private_bytes=memory.private_bytes,
                working_set=memory.working_set, handles=count.value)
        finally:
            kernel.CloseHandle(handle)
    return result
