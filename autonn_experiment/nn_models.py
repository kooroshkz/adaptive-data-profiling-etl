"""Non-linear anomaly detectors: Autoencoder, VAE (PyTorch), and OCSVM (sklearn).

All detectors expose a sklearn-style interface:
    fit(X_train: np.ndarray) -> self
    predict(X: np.ndarray) -> np.ndarray  (0=normal, 1=anomaly)

Reconstruction error (AE/VAE) or decision function (OCSVM) is thresholded at
the (1 - contamination) quantile of training-set scores, matching PyOD convention.
"""
from __future__ import annotations

from typing import List

import numpy as np

# ── PyTorch models (AE, VAE) ──────────────────────────────────────────────────

import torch
import torch.nn as nn


def _activation(name: str) -> nn.Module:
    return {"relu": nn.ReLU(), "tanh": nn.Tanh(), "elu": nn.ELU(), "leaky_relu": nn.LeakyReLU(0.1)}[name]


def _mlp(dims: List[int], act: nn.Module, final_act: bool = True) -> nn.Sequential:
    layers: list[nn.Module] = []
    for i in range(len(dims) - 1):
        layers.append(nn.Linear(dims[i], dims[i + 1]))
        if final_act or i < len(dims) - 2:
            layers.append(act)
    return nn.Sequential(*layers)


class _AENet(nn.Module):
    def __init__(self, input_dim: int, hidden_dims: List[int], act: nn.Module):
        super().__init__()
        enc_dims = [input_dim] + hidden_dims
        dec_dims = list(reversed(hidden_dims)) + [input_dim]
        self.enc = _mlp(enc_dims, act)
        self.dec = _mlp(dec_dims, act, final_act=False)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.dec(self.enc(x))


class _VAENet(nn.Module):
    def __init__(self, input_dim: int, hidden_dims: List[int], act: nn.Module):
        super().__init__()
        latent_dim = max(2, hidden_dims[-1])
        enc_body_dims = [input_dim] + hidden_dims[:-1]
        enc_body_out = enc_body_dims[-1] if len(enc_body_dims) > 1 else input_dim

        self.enc_body = _mlp(enc_body_dims, act) if len(enc_body_dims) > 1 else nn.Identity()
        self.fc_mu = nn.Linear(enc_body_out, latent_dim)
        self.fc_lv = nn.Linear(enc_body_out, latent_dim)

        dec_dims = [latent_dim] + list(reversed(hidden_dims[:-1])) + [input_dim]
        self.dec = _mlp(dec_dims, act, final_act=False)

    def forward(self, x: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        h = self.enc_body(x)
        mu, lv = self.fc_mu(h), self.fc_lv(h)
        z = mu + torch.exp(0.5 * lv) * torch.randn_like(mu) if self.training else mu
        return self.dec(z), mu, lv


def _vae_loss(recon: torch.Tensor, x: torch.Tensor, mu: torch.Tensor, lv: torch.Tensor) -> torch.Tensor:
    recon_loss = torch.mean((recon - x) ** 2, dim=1)
    kl = -0.5 * torch.sum(1 + lv - mu.pow(2) - lv.exp(), dim=1)
    return (recon_loss + 0.001 * kl).mean()


class AutoencoderDetector:
    """Autoencoder-based anomaly detector with Optuna-searchable architecture."""

    def __init__(
        self,
        input_dim: int,
        hidden_dims: List[int],
        activation: str = "relu",
        lr: float = 1e-3,
        epochs: int = 50,
        batch_size: int = 256,
        contamination: float = 0.05,
        seed: int = 42,
    ):
        torch.manual_seed(seed)
        np.random.seed(seed)
        self.contamination = contamination
        self.lr = lr
        self.epochs = epochs
        self.batch_size = batch_size
        self.threshold_: float | None = None
        act = _activation(activation)
        self.net = _AENet(input_dim, hidden_dims, act)
        self.opt = torch.optim.Adam(self.net.parameters(), lr=lr)

    def fit(self, X: np.ndarray) -> "AutoencoderDetector":
        X_t = torch.FloatTensor(X)
        self.net.train()
        n = len(X_t)
        for _ in range(self.epochs):
            perm = torch.randperm(n)
            for start in range(0, n, self.batch_size):
                b = X_t[perm[start: start + self.batch_size]]
                loss = torch.mean((self.net(b) - b) ** 2)
                self.opt.zero_grad()
                loss.backward()
                self.opt.step()
        scores = self._score(X_t)
        self.threshold_ = float(np.quantile(scores, 1.0 - self.contamination))
        return self

    def _score(self, X_t: torch.Tensor) -> np.ndarray:
        self.net.eval()
        with torch.no_grad():
            recon = self.net(X_t)
            return torch.mean((recon - X_t) ** 2, dim=1).numpy()

    def predict(self, X: np.ndarray) -> np.ndarray:
        scores = self._score(torch.FloatTensor(X))
        return (scores > self.threshold_).astype(int)


class VAEDetector:
    """Variational Autoencoder anomaly detector (ELBO-trained, scored by MSE recon)."""

    def __init__(
        self,
        input_dim: int,
        hidden_dims: List[int],
        activation: str = "relu",
        lr: float = 1e-3,
        epochs: int = 50,
        batch_size: int = 256,
        contamination: float = 0.05,
        seed: int = 42,
    ):
        torch.manual_seed(seed)
        np.random.seed(seed)
        self.contamination = contamination
        self.lr = lr
        self.epochs = epochs
        self.batch_size = batch_size
        self.threshold_: float | None = None
        act = _activation(activation)
        self.net = _VAENet(input_dim, hidden_dims, act)
        self.opt = torch.optim.Adam(self.net.parameters(), lr=lr)

    def fit(self, X: np.ndarray) -> "VAEDetector":
        X_t = torch.FloatTensor(X)
        self.net.train()
        n = len(X_t)
        for _ in range(self.epochs):
            perm = torch.randperm(n)
            for start in range(0, n, self.batch_size):
                b = X_t[perm[start: start + self.batch_size]]
                recon, mu, lv = self.net(b)
                loss = _vae_loss(recon, b, mu, lv)
                self.opt.zero_grad()
                loss.backward()
                self.opt.step()
        scores = self._score(X_t)
        self.threshold_ = float(np.quantile(scores, 1.0 - self.contamination))
        return self

    def _score(self, X_t: torch.Tensor) -> np.ndarray:
        self.net.eval()
        with torch.no_grad():
            recon, _, _ = self.net(X_t)
            return torch.mean((recon - X_t) ** 2, dim=1).numpy()

    def predict(self, X: np.ndarray) -> np.ndarray:
        scores = self._score(torch.FloatTensor(X))
        return (scores > self.threshold_).astype(int)


# ── One-Class SVM ─────────────────────────────────────────────────────────────

from sklearn.svm import OneClassSVM


class OCSVMDetector:
    """One-Class SVM (RBF kernel). Sub-samples training data to cap O(n²) cost."""

    def __init__(
        self,
        nu: float = 0.05,
        gamma: str | float = "scale",
        max_train_samples: int = 3_000,
        contamination: float = 0.05,
        seed: int = 42,
    ):
        self.max_train_samples = max_train_samples
        self.contamination = contamination
        self.rng = np.random.default_rng(seed)
        self.clf = OneClassSVM(kernel="rbf", nu=nu, gamma=gamma)
        self.threshold_: float | None = None

    def fit(self, X: np.ndarray) -> "OCSVMDetector":
        if len(X) > self.max_train_samples:
            idx = self.rng.choice(len(X), self.max_train_samples, replace=False)
            X_fit = X[idx]
        else:
            X_fit = X
        self.clf.fit(X_fit)
        # sklearn decision_function: positive = normal, negative = anomaly
        # Negate so higher score = more anomalous, then threshold at quantile
        scores = -self.clf.decision_function(X)
        self.threshold_ = float(np.quantile(scores, 1.0 - self.contamination))
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        scores = -self.clf.decision_function(X)
        return (scores > self.threshold_).astype(int)


# ── Factory ───────────────────────────────────────────────────────────────────

def build_detector(model_name: str, params: dict, input_dim: int, seed: int = 42):
    """Instantiate a detector from Optuna-suggested params."""
    if model_name == "AE":
        return AutoencoderDetector(
            input_dim=input_dim,
            hidden_dims=params["hidden_dims"],
            activation=params["activation"],
            lr=params["lr"],
            epochs=params["epochs"],
            batch_size=params.get("batch_size", 256),
            contamination=params["contamination"],
            seed=seed,
        )
    if model_name == "VAE":
        return VAEDetector(
            input_dim=input_dim,
            hidden_dims=params["hidden_dims"],
            activation=params["activation"],
            lr=params["lr"],
            epochs=params["epochs"],
            batch_size=params.get("batch_size", 256),
            contamination=params["contamination"],
            seed=seed,
        )
    if model_name == "OCSVM":
        return OCSVMDetector(
            nu=params["nu"],
            gamma=params["gamma"],
            max_train_samples=params.get("max_train_samples", 3_000),
            contamination=params["contamination"],
            seed=seed,
        )
    raise ValueError(f"Unknown model: {model_name}")
