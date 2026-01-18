"""
settings.py
Thin wrapper around existing settings persistence and DB-backed settings_system.
Exposes: load_settings, save_settings, apply_admin_theme, get_business_info, get_logo_image
"""
from pathlib import Path
import os

try:
    from settings_persistence import load_settings as _load_json_settings, save_settings as _save_json_settings
except Exception:
    def _load_json_settings():
        return {}
    def _save_json_settings(s):
        return True

try:
    import settings_system as _settings_system
except Exception:
    _settings_system = None

# Module-level cache (lightweight)
_APP_SETTINGS = None


def load_settings():
    """Load combined settings (JSON printer/settings + DB business info)."""
    global _APP_SETTINGS
    json_s = {}
    try:
        json_s = _load_json_settings() or {}
    except Exception:
        json_s = {}

    db_s = {}
    try:
        if _settings_system:
            db_s = _settings_system.get_settings() or {}
    except Exception:
        db_s = {}

    # Merge: DB business info (db_s) wins for business fields; printer/json stays
    merged = {}
    merged.update(json_s)
    merged.update(db_s)
    _APP_SETTINGS = merged
    return _APP_SETTINGS


def save_settings(settings_dict: dict):
    """Save both DB-backed business settings and JSON-backed runtime settings (printer etc.).
    Returns True on success.
    """
    ok_db = True
    ok_json = True
    try:
        if _settings_system and any(k in settings_dict for k in ('business_name','phone','email','receipt_tagline','logo_path','dark_mode','excel_header','address','tpin')):
            ok_db = _settings_system.save_settings(settings_dict)
    except Exception:
        ok_db = False
    try:
        # Save printer-specific/nested settings to JSON if present
        ok_json = _save_json_settings({k: v for k, v in settings_dict.items() if k not in ('business_name','phone','email','receipt_tagline','logo_path','dark_mode','excel_header','address','tpin')})
    except Exception:
        ok_json = False
    # Refresh cache
    try:
        load_settings()
    except Exception:
        pass
    return ok_db and ok_json


def apply_admin_theme(root_widget, settings=None):
    """Apply admin-only theme based on dark_mode flag. Attempts to update common widgets' bg/fg and ttk styles.
    This only affects the provided root_widget and its children.
    """
    try:
        s = settings if settings is not None else load_settings()
        dark = bool(int(s.get('dark_mode', 0))) if isinstance(s.get('dark_mode', 0), (str, int)) else bool(s.get('dark_mode', False))
    except Exception:
        dark = False

    # Basic palettes
    if dark:
        bg = '#2b2b2b'
        fg = '#ecf0f1'
        btn_bg = '#3a3f44'
        btn_fg = '#ecf0f1'
        entry_bg = '#3a3a3a'
    else:
        bg = '#ecf0f1'
        fg = '#2c3e50'
        btn_bg = '#3498db'
        btn_fg = '#ffffff'
        entry_bg = '#ffffff'

    # Apply to root
    try:
        root_widget.configure(bg=bg)
    except Exception:
        pass

    # Update ttk styles where possible
    try:
        import tkinter.ttk as ttk
        style = ttk.Style()
        # Keep existing theme but configure element colors
        try:
            style.configure('TButton', background=btn_bg, foreground=btn_fg)
            style.configure('TLabel', background=bg, foreground=fg)
            style.configure('TFrame', background=bg)
            style.configure('Treeview', background=entry_bg, fieldbackground=entry_bg, foreground=fg)
        except Exception:
            pass
    except Exception:
        pass

    # Recursively update common tk widgets
    def _recurse(widget):
        for child in widget.winfo_children():
            cls = child.__class__.__name__
            try:
                if cls in ('Frame','LabelFrame'):
                    child.configure(bg=bg)
                elif cls == 'Label':
                    child.configure(bg=bg, fg=fg)
                elif cls == 'Button':
                    try:
                        child.configure(bg=btn_bg, fg=btn_fg)
                    except Exception:
                        pass
                elif cls == 'Entry':
                    try:
                        child.configure(bg=entry_bg, fg=fg)
                    except Exception:
                        pass
                elif cls in ('Text','Listbox'):
                    try:
                        child.configure(bg=entry_bg, fg=fg)
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                _recurse(child)
            except Exception:
                pass
    try:
        _recurse(root_widget)
    except Exception:
        pass


def get_business_info():
    """Return business info used for receipts and exports (wrap settings_system.get_receipt_info)."""
    try:
        if _settings_system:
            return _settings_system.get_receipt_info()
    except Exception:
        pass
    # Fallback defaults
    return {
        'business_name': 'Gorgeous Brides Boutique',
        'address': 'Shop F14 Upstairs, Downtown Shopping Mall',
        'phone': '+260779370289',
        'email': 'comfortlimata@gmail.com',
        'tpin': '1018786730',
        'tagline': 'Thank you for shopping with us!',
        'logo_path': 'assets/logo.png'
    }


def get_logo_image(max_size=(150,100)):
    """Return a PIL Image object resized to max_size or None."""
    try:
        path = get_business_info().get('logo_path')
        if not path:
            return None
        from PIL import Image
        img = Image.open(path)
        img.thumbnail(max_size)
        return img
    except Exception:
        return None

