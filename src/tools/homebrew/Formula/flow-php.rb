class FlowPhp < Formula
  desc "Flow is a first and the most advanced PHP ETL framework"
  homepage "https://github.com/flow-php/flow"
  url "https://github.com/flow-php/flow/releases/download/0.7.0/flow.phar"
  sha256 "6c4a3e1052cbdb152227fe371ec2a7d63b098638bfacb0ba64ba2405a7866170"
  license "MIT"

  depends_on "php"

  def install
    bin.install "flow.phar" => "flow"
  end

  test do
    shell_output("#{bin}/flow --version").include?(version)
  end
end
