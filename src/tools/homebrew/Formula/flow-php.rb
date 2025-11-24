class FlowPhp < Formula
  desc "Flow is a first and the most advanced PHP ETL framework"
  homepage "https://github.com/flow-php/flow"
  url "https://github.com/flow-php/flow/releases/download/0.28.0/flow.phar"
  sha256 "5e279a62e6ccb7e26e5d0be99eeda914c36e0fe4bb2ffabee7cc19065ac0a283"
  license "MIT"

  depends_on "php"

  def install
    bin.install "flow.phar" => "flow"
  end

  test do
    shell_output("#{bin}/flow --version").include?(version)
  end
end
