import re

RULES = {
    "影像生成": ["stable diffusion", "midjourney", "text-to-image", "controlnet", "image", "diffusion"],
    "聊天機器人": ["chatgpt", "assistant", "chatbot", "llm", "qa", "rag"],
    "程式碼輔助": ["code", "copilot", "vscode", "lint", "debug"],
    "語音/音樂": ["audio", "tts", "asr", "whisper", "music", "voice"],
    "影片生成/剪輯": ["video", "sora", "clip", "subtitle", "transcribe"],
    "多模態/代理": ["agent", "mcp", "tool-use", "multimodal"],
}

def classify(name: str, description: str = "", topics: str = "") -> str:
    hay = f"{name} {description} {topics}".lower()
    for cat, kws in RULES.items():
        if any(re.search(rf"\b{k}\b", hay) for k in kws):
            return cat
    return "其他"
