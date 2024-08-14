{
  pkgs,
  lib,
  config,
  ...
}:
let
  cfg = config.electrack;

  electrack = pkgs.rustPlatform.buildRustPackage {
    pname = "electrack";
    version = "0.0.4";
    src = ./.;
    cargoLock = {
      lockFile = ./Cargo.lock;
    };

    nativeBuildInputs = [ pkgs.pkg-config ];
    PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";
  };
in
{

  options = {
    electrack.enable = lib.mkEnableOption "Enable electrack";
  };

  config = lib.mkIf cfg.enable { environment.systemPackages = [ electrack ]; };

}
