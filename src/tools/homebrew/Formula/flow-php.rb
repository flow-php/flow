class FlowPhp < Formula
  desc "Flow is a first and the most advanced PHP ETL framework"
  homepage "https://github.com/flow-php/flow"
  url "https://github.com/flow-php/flow/releases/download/0.19.0/flow.phar"
  sha256 "d23561e34577d2711b3a03daecc2c94fa3e34c81c2a3d8b91931dbec6e08a1a6"
  license "MIT"

  depends_on "php"

  def install
    bin.install "flow.phar" => "flow"
  end

  test do
    shell_output("#{bin}/flow --version").include?(version)
  end
end
