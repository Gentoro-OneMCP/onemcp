#!/bin/bash
set -e

echo "OneMCP Uninstaller"
echo "=================="
echo

# Check if running with sudo for /usr/local/bin
if [ "$EUID" -ne 0 ] && [ -f "/usr/local/bin/onemcp" ]; then
    echo "Note: You may need to run with sudo to remove /usr/local/bin/onemcp"
    echo "Usage: sudo ./uninstall.sh"
    echo
fi

echo "This will remove:"
echo "  - OneMCP binary (/usr/local/bin/onemcp)"
echo "  - Configuration (~/.onemcp/)"
echo "  - Handbooks (~/onemcp-handbooks/)"
echo "  - Docker images (admingentoro/gentoro)"
echo

# Confirm
read -p "Continue? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Uninstall cancelled."
    exit 0
fi

echo

# Stop server if running
echo "⏸️  Stopping OneMCP server..."
if command -v onemcp &> /dev/null; then
    onemcp stop 2>/dev/null || true
fi

# Remove binary
echo "🗑️  Removing binary..."
if [ -f "/usr/local/bin/onemcp" ]; then
    rm -f /usr/local/bin/onemcp
    echo "   ✓ Removed /usr/local/bin/onemcp"
else
    echo "   ℹ️  Binary not found in /usr/local/bin"
fi

# Remove configuration
echo "🗑️  Removing configuration..."
if [ -d "$HOME/.onemcp" ]; then
    rm -rf "$HOME/.onemcp"
    echo "   ✓ Removed ~/.onemcp/"
else
    echo "   ℹ️  Configuration directory not found"
fi

# Ask about handbooks
if [ -d "$HOME/onemcp-handbooks" ]; then
    echo
    echo "📚 Found handbooks directory: ~/onemcp-handbooks/"
    read -p "   Remove handbooks? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -rf "$HOME/onemcp-handbooks"
        echo "   ✓ Removed ~/onemcp-handbooks/"
    else
        echo "   ℹ️  Keeping ~/onemcp-handbooks/"
    fi
fi

# Ask about Docker images
echo
if command -v docker &> /dev/null; then
    if docker images | grep -q "admingentoro/gentoro"; then
        echo "🐳 Found Docker image: admingentoro/gentoro"
        read -p "   Remove Docker image? (y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            docker rmi admingentoro/gentoro:latest 2>/dev/null || true
            echo "   ✓ Removed Docker image"
        else
            echo "   ℹ️  Keeping Docker image"
        fi
    fi
fi

echo
echo "✅ OneMCP uninstalled successfully!"
echo
echo "To reinstall:"
echo "  curl -fsSL https://raw.githubusercontent.com/Gentoro-OneMCP/onemcp/main/packages/go-cli/install.sh | bash"
