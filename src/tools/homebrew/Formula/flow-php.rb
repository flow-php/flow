class FlowPhp < Formula
  desc "Flow is a first and the most advanced PHP ETL framework"
  homepage "https://github.com/flow-php/flow"
  url "https://github.com/flow-php/flow/releases/download/0.16.1/flow.phar"
  sha256 "3d1a51a9547f0e6bcf3ce6470b2426b0de7ba9e29aebb4d9b6ce1f1b62fe8ab8"
  license "MIT"

  depends_on "php"

  def install
    bin.install "flow.phar" => "flow"
  end

  test do
    shell_output("#{bin}/flow --version").include?(version)
  end
end
