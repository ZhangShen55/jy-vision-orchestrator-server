from dataclasses import dataclass
from pathlib import Path

import cv2


@dataclass(frozen=True)
class SnapshotSaveResult:
    relative_path: str
    absolute_path: Path


class SnapshotStorage:
    def __init__(self, mount_root: Path, relative_prefix: str = "cv", scale: float = 0.25):
        self.mount_root = Path(mount_root)
        self.relative_prefix = relative_prefix.strip("/")
        self.scale = float(scale)

    def ensure_writable(self) -> None:
        if not self.mount_root.exists():
            raise FileNotFoundError(f"NFS 挂载目录不存在: {self.mount_root}")
        if not self.mount_root.is_dir():
            raise NotADirectoryError(f"NFS 挂载路径不是目录: {self.mount_root}")
        probe = self.mount_root / ".vision_orchestrator_write_probe"
        try:
            probe.write_text("ok", encoding="utf-8")
        finally:
            try:
                probe.unlink()
            except FileNotFoundError:
                pass

    def build_relative_path(self, task_id: str, image_id: str) -> str:
        return f"{self.relative_prefix}/{task_id}/{image_id}.png"

    def save_snapshot(self, task_id: str, image_id: str, image) -> SnapshotSaveResult:
        relative_path = self.build_relative_path(task_id, image_id)
        absolute_path = self.mount_root / relative_path
        absolute_path.parent.mkdir(parents=True, exist_ok=True)
        if self.scale <= 0:
            raise ValueError("snapshot scale must be positive")
        resized = cv2.resize(image, None, fx=self.scale, fy=self.scale, interpolation=cv2.INTER_AREA)
        ok = cv2.imwrite(str(absolute_path), resized)
        if not ok:
            raise IOError(f"抓拍图片写入失败: {absolute_path}")
        return SnapshotSaveResult(relative_path=relative_path, absolute_path=absolute_path)

    @staticmethod
    def read_image(path: Path):
        image = cv2.imread(str(path))
        if image is None:
            raise IOError(f"读取图片失败: {path}")
        return image
