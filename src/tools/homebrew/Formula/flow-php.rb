class FlowPhp < Formula
  desc "Flow is a first and the most advanced PHP ETL framework"
  homepage "https://github.com/flow-php/flow"
  url "https://github.com/flow-php/flow/releases/download/0.25.0/flow.phar"
  sha256 "d65505fd8fa358474c726c8bd5e1cf40b952ce4852fa8956aade19dee98aa0f0"
  license "MIT"

  depends_on "php"

  def install
    bin.install "flow.phar" => "flow"
  end

  test do
    shell_output("#{bin}/flow --version").include?(version)
  end
end
