{
	inputs = {
		# Required for Nix 2.27.0+ when using git submodules
		self.submodules = true;

		nixpkgs.url = "github:nixos/nixpkgs/nixos-25.05";
		flake-utils.url = "github:numtide/flake-utils";
		dev-shell.url = "github:biobricks-ai/dev-shell";

		biobricks-script-lib = {
			url = "github:biobricks-ai/biobricks-script-lib";
			inputs.nixpkgs.follows = "nixpkgs";
			inputs.flake-utils.follows = "flake-utils";
		};
	};

	outputs = { self, nixpkgs, flake-utils, biobricks-script-lib, dev-shell }:
		flake-utils.lib.eachDefaultSystem (system:
			with import nixpkgs { inherit system; }; {
				devShells.default = dev-shell.devShells.${system}.default.overrideAttrs
        (oldAttrs: {
					buildInputs = [
					] ++ biobricks-script-lib.packages.${system}.buildInputs;

					shellHook = ''
						# Activate biobricks-script-lib environment
						${biobricks-script-lib.devShells.${system}.default.shellHook or ""}

            source .venv/bin/activate
					'';
				});
			});
}