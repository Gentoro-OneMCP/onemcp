class Onemcp < Formula
  desc "CLI for Gentoro OneMCP - Connect APIs to AI models via Model Context Protocol"
  homepage "https://github.com/Gentoro-OneMCP/onemcp"
  license "Apache-2.0"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/Gentoro-OneMCP/onemcp/releases/download/v0.1.0/onemcp-darwin-arm64.tar.gz"
      sha256 "3b8a8d0760ca53e020bdbe2cb51151407cf086b049051f4df315c489f9938af7"
    else
      url "https://github.com/Gentoro-OneMCP/onemcp/releases/download/v0.1.0/onemcp-darwin-amd64.tar.gz"
      sha256 "cf3d16595f14a7105f318e705a0dca9dab5a055825ce1e44507797cee340a008"
    end
  end

  on_linux do
    url "https://github.com/Gentoro-OneMCP/onemcp/releases/download/v0.1.0/onemcp-linux-amd64.tar.gz"
    sha256 "4c297f1be7d5dc3a2a2ecfc69a51050c801b74ab5ff4bb1d816cc55bbc393b57"
  end

  depends_on "docker" => :recommended

  def install
    bin.install "onemcp"
  end

  def caveats
    <<~EOS
      OneMCP requires Docker to run the server.
      
      Get started:
        onemcp chat
      
      View all commands:
        onemcp --help
    EOS
  end

  test do
    assert_match "Gentoro One MCP CLI", shell_output("#{bin}/onemcp --help")
  end
end
