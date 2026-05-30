#!/bin/bash

set -e

VERSION="1.0"
REPO="jeff2009wang/frp_python"
DOWNLOAD_BASE="https://github.com/${REPO}/releases/download"
RAW_BASE="https://raw.githubusercontent.com/${REPO}/main"

INSTALL_DIR="/usr/local/bin"
CONFIG_DIR="/etc/pfrp"
SERVICE_DIR="/etc/systemd/system"
LOG_DIR="/var/log/pfrp"

INSTALL_MODE=""
USE_MIRROR=""

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_header() {
    echo -e "${BLUE}"
    echo "=========================================="
    echo "    PFRP Installer v${VERSION}"
    echo "    Multi-Connection FRP Client/Server"
    echo "=========================================="
    echo -e "${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

check_root() {
    if [[ $EUID -ne 0 ]]; then
        print_error "This script must be run as root"
        exit 1
    fi
}

detect_arch() {
    ARCH=$(uname -m)
    case $ARCH in
        x86_64)
            BINARY_ARCH="amd64"
            ;;
        aarch64)
            BINARY_ARCH="arm64"
            ;;
        armv7l)
            BINARY_ARCH="armv7"
            ;;
        *)
            print_error "Unsupported architecture: $ARCH"
            exit 1
            ;;
    esac
    print_success "Detected architecture: $ARCH ($BINARY_ARCH)"
}

get_download_url() {
    local file=$1
    if [ "$USE_MIRROR" = "true" ]; then
        echo "https://gh-proxy.com/${RAW_BASE}/dist/linux/${file}"
    else
        echo "https://github.com/${REPO}/raw/main/dist/linux/${file}"
    fi
}

download_file() {
    local url=$1
    local output=$2
    if command -v wget &> /dev/null; then
        wget -q --show-progress "$url" -O "$output"
    elif command -v curl &> /dev/null; then
        curl -fsSL --progress-bar "$url" -o "$output"
    else
        print_error "Neither wget nor curl is installed. Please install one of them."
        exit 1
    fi
}

install_client() {
    print_info "Installing PFRP Client..."
    
    cd /tmp
    rm -rf pfrp_install
    mkdir -p pfrp_install
    cd pfrp_install
    
    local download_url=$(get_download_url "frpc_multi")
    
    if [ "$USE_MIRROR" = "true" ]; then
        print_info "Downloading frpc_multi from GitHub mirror (accelerated)..."
    else
        print_info "Downloading frpc_multi from GitHub..."
    fi
    
    download_file "${download_url}" frpc_multi || {
        print_error "Failed to download frpc_multi"
        if [ "$USE_MIRROR" != "true" ]; then
            print_warning "Retrying with mirror..."
            USE_MIRROR="true"
            download_url=$(get_download_url "frpc_multi")
            download_file "${download_url}" frpc_multi || {
                print_error "Failed to download from mirror as well"
                exit 1
            }
        else
            exit 1
        fi
    }
    
    chmod +x frpc_multi
    mv frpc_multi ${INSTALL_DIR}/pfrpc
    print_success "Installed pfrpc to ${INSTALL_DIR}"
    
    create_client_service
    create_pfrp_command "client"
    print_success "PFRP Client installation complete!"
    show_client_usage
}

install_server() {
    print_info "Installing PFRP Server..."
    
    cd /tmp
    rm -rf pfrp_install
    mkdir -p pfrp_install
    cd pfrp_install
    
    local download_url=$(get_download_url "frps_multi")
    
    if [ "$USE_MIRROR" = "true" ]; then
        print_info "Downloading frps_multi from GitHub mirror (accelerated)..."
    else
        print_info "Downloading frps_multi from GitHub..."
    fi
    
    download_file "${download_url}" frps_multi || {
        print_error "Failed to download frps_multi"
        if [ "$USE_MIRROR" != "true" ]; then
            print_warning "Retrying with mirror..."
            USE_MIRROR="true"
            download_url=$(get_download_url "frps_multi")
            download_file "${download_url}" frps_multi || {
                print_error "Failed to download from mirror as well"
                exit 1
            }
        else
            exit 1
        fi
    }
    
    chmod +x frps_multi
    mv frps_multi ${INSTALL_DIR}/pfrps
    print_success "Installed pfrps to ${INSTALL_DIR}"
    
    create_server_service
    create_pfrp_command "server"
    print_success "PFRP Server installation complete!"
    show_server_usage
}

create_client_service() {
    cat > ${SERVICE_DIR}/pfrpc.service << EOF
[Unit]
Description=PFRP Multi-Connection Client
After=network.target

[Service]
Type=simple
User=root
ExecStart=${INSTALL_DIR}/pfrpc 127.0.0.1 7000 --channels 16
Restart=on-failure
RestartSec=5s
StandardOutput=append:${LOG_DIR}/pfrpc.log
StandardError=append:${LOG_DIR}/pfrpc.error.log

[Install]
WantedBy=multi-user.target
EOF
    
    systemctl daemon-reload
    print_success "Created systemd service: pfrpc.service"
}

create_server_service() {
    cat > ${SERVICE_DIR}/pfrps.service << EOF
[Unit]
Description=PFRP Multi-Connection Server
After=network.target

[Service]
Type=simple
User=root
ExecStart=${INSTALL_DIR}/pfrps 7000 --channels 16
Restart=on-failure
RestartSec=5s
StandardOutput=append:${LOG_DIR}/pfrps.log
StandardError=append:${LOG_DIR}/pfrps.error.log

[Install]
WantedBy=multi-user.target
EOF
    
    systemctl daemon-reload
    print_success "Created systemd service: pfrps.service"
}

create_pfrp_command() {
    local mode=$1
    
    cat > ${INSTALL_DIR}/pfrp << 'EOF'
#!/bin/bash

PFRP_CLIENT_SERVICE="pfrpc.service"
PFRP_SERVER_SERVICE="pfrps.service"
INSTALL_DIR="/usr/local/bin"

print_status() {
    echo "=== PFRP Status ==="
    echo ""
    
    if systemctl is-active --quiet ${PFRP_CLIENT_SERVICE} 2>/dev/null; then
        echo -e "✓ Client: \033[0;32mRunning\033[0m"
        systemctl status ${PFRP_CLIENT_SERVICE} --no-pager -l | grep -E "Active:|Main PID:" | head -2
    elif [ -f "${SERVICE_DIR}/${PFRP_CLIENT_SERVICE}" ]; then
        echo -e "✗ Client: \033[0;31mStopped\033[0m"
    else
        echo "  Client: Not installed"
    fi
    
    echo ""
    
    if systemctl is-active --quiet ${PFRP_SERVER_SERVICE} 2>/dev/null; then
        echo -e "✓ Server: \033[0;32mRunning\033[0m"
        systemctl status ${PFRP_SERVER_SERVICE} --no-pager -l | grep -E "Active:|Main PID:" | head -2
    elif [ -f "${SERVICE_DIR}/${PFRP_SERVER_SERVICE}" ]; then
        echo -e "✗ Server: \033[0;31mStopped\033[0m"
    else
        echo "  Server: Not installed"
    fi
    
    echo ""
    echo "=== Recent Logs ==="
    
    if systemctl is-active --quiet ${PFRP_CLIENT_SERVICE} 2>/dev/null; then
        echo ""
        echo "--- Client Logs (last 10 lines) ---"
        journalctl -u ${PFRP_CLIENT_SERVICE} -n 10 --no-pager
    fi
    
    if systemctl is-active --quiet ${PFRP_SERVER_SERVICE} 2>/dev/null; then
        echo ""
        echo "--- Server Logs (last 10 lines) ---"
        journalctl -u ${PFRP_SERVER_SERVICE} -n 10 --no-pager
    fi
}

show_usage() {
    cat << HELP
PFRP Management Tool v1.0

Usage: pfrp [command] [options]

Commands:
  status              Show service status and recent logs
  start [client|server]   Start client or server service
  stop [client|server]    Stop client or server service
  restart [client|server] Restart client or server service
  enable [client|server]  Enable auto-start on boot
  disable [client|server] Disable auto-start on boot
  logs [client|server]    Show service logs (follow mode)
  config              Show configuration location
  version             Show version information
  
Examples:
  pfrp status              Show all service status
  pfrp start client        Start client service
  pfrp stop server         Stop server service
  pfrp logs client         Follow client logs
  pfrp restart client      Restart client service

HELP
}

show_version() {
    cat << VERSION
PFRP Multi-Connection FRP v1.0
Client: pfrpc
Server: pfrps
VERSION
}

case "$1" in
    status)
        print_status
        ;;
    start)
        if [ -z "$2" ]; then
            echo "Error: Please specify 'client' or 'server'"
            echo "Usage: pfrp start [client|server]"
            exit 1
        fi
        if [ "$2" = "client" ]; then
            systemctl start ${PFRP_CLIENT_SERVICE}
            echo "PFRP Client started"
        elif [ "$2" = "server" ]; then
            systemctl start ${PFRP_SERVER_SERVICE}
            echo "PFRP Server started"
        else
            echo "Error: Invalid option. Use 'client' or 'server'"
            exit 1
        fi
        ;;
    stop)
        if [ -z "$2" ]; then
            echo "Error: Please specify 'client' or 'server'"
            echo "Usage: pfrp stop [client|server]"
            exit 1
        fi
        if [ "$2" = "client" ]; then
            systemctl stop ${PFRP_CLIENT_SERVICE}
            echo "PFRP Client stopped"
        elif [ "$2" = "server" ]; then
            systemctl stop ${PFRP_SERVER_SERVICE}
            echo "PFRP Server stopped"
        else
            echo "Error: Invalid option. Use 'client' or 'server'"
            exit 1
        fi
        ;;
    restart)
        if [ -z "$2" ]; then
            echo "Error: Please specify 'client' or 'server'"
            echo "Usage: pfrp restart [client|server]"
            exit 1
        fi
        if [ "$2" = "client" ]; then
            systemctl restart ${PFRP_CLIENT_SERVICE}
            echo "PFRP Client restarted"
        elif [ "$2" = "server" ]; then
            systemctl restart ${PFRP_SERVER_SERVICE}
            echo "PFRP Server restarted"
        else
            echo "Error: Invalid option. Use 'client' or 'server'"
            exit 1
        fi
        ;;
    enable)
        if [ -z "$2" ]; then
            echo "Error: Please specify 'client' or 'server'"
            echo "Usage: pfrp enable [client|server]"
            exit 1
        fi
        if [ "$2" = "client" ]; then
            systemctl enable ${PFRP_CLIENT_SERVICE}
            echo "PFRP Client enabled to start on boot"
        elif [ "$2" = "server" ]; then
            systemctl enable ${PFRP_SERVER_SERVICE}
            echo "PFRP Server enabled to start on boot"
        else
            echo "Error: Invalid option. Use 'client' or 'server'"
            exit 1
        fi
        ;;
    disable)
        if [ -z "$2" ]; then
            echo "Error: Please specify 'client' or 'server'"
            echo "Usage: pfrp disable [client|server]"
            exit 1
        fi
        if [ "$2" = "client" ]; then
            systemctl disable ${PFRP_CLIENT_SERVICE}
            echo "PFRP Client disabled from auto-start"
        elif [ "$2" = "server" ]; then
            systemctl disable ${PFRP_SERVER_SERVICE}
            echo "PFRP Server disabled from auto-start"
        else
            echo "Error: Invalid option. Use 'client' or 'server'"
            exit 1
        fi
        ;;
    logs)
        if [ -z "$2" ]; then
            echo "Error: Please specify 'client' or 'server'"
            echo "Usage: pfrp logs [client|server]"
            exit 1
        fi
        if [ "$2" = "client" ]; then
            journalctl -u ${PFRP_CLIENT_SERVICE} -f
        elif [ "$2" = "server" ]; then
            journalctl -u ${PFRP_SERVER_SERVICE} -f
        else
            echo "Error: Invalid option. Use 'client' or 'server'"
            exit 1
        fi
        ;;
    config)
        echo "Configuration files:"
        echo "  Client: ${SERVICE_DIR}/pfrpc.service"
        echo "  Server: ${SERVICE_DIR}/pfrps.service"
        echo "  Logs: /var/log/pfrp/"
        ;;
    version)
        show_version
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        if [ -z "$1" ]; then
            print_status
        else
            echo "Error: Unknown command '$1'"
            echo "Run 'pfrp help' for usage information"
            exit 1
        fi
        ;;
esac
EOF
    
    chmod +x ${INSTALL_DIR}/pfrp
    print_success "Created pfrp management command"
}

show_client_usage() {
    cat << EOF

${GREEN}Client Installation Complete!${NC}

${YELLOW}Service Management:${NC}
  Start:   systemctl start pfrpc
  Stop:    systemctl stop pfrpc
  Restart: systemctl restart pfrpc
  Enable:  systemctl enable pfrpc  (auto-start on boot)

${YELLOW}Using pfrp command:${NC}
  pfrp status              Show service status
  pfrp start client        Start client
  pfrp stop client         Stop client
  pfrp logs client         View logs (follow mode)

${YELLOW}Configuration:${NC}
  Edit: ${SERVICE_DIR}/pfrpc.service
  Reload: systemctl daemon-reload && systemctl restart pfrpc

${YELLOW}Logs:${NC}
  View:  journalctl -u pfrpc -n 50
  Follow: journalctl -u pfrpc -f

${YELLOW}Manual Usage:${NC}
  pfrpc <server_host> <server_port> [--channels NUM] [--target HOST] [--interval SECS] [--ports PORTS]

EOF
}

show_server_usage() {
    cat << EOF

${GREEN}Server Installation Complete!${NC}

${YELLOW}Service Management:${NC}
  Start:   systemctl start pfrps
  Stop:    systemctl stop pfrps
  Restart: systemctl restart pfrps
  Enable:  systemctl enable pfrps  (auto-start on boot)

${YELLOW}Using pfrp command:${NC}
  pfrp status              Show service status
  pfrp start server        Start server
  pfrp stop server         Stop server
  pfrp logs server         View logs (follow mode)

${YELLOW}Configuration:${NC}
  Edit: ${SERVICE_DIR}/pfrps.service
  Reload: systemctl daemon-reload && systemctl restart pfrps

${YELLOW}Logs:${NC}
  View:  journalctl -u pfrps -n 50
  Follow: journalctl -u pfrps -f

${YELLOW}Manual Usage:${NC}
  pfrps <port> [--channels NUM] [--host HOST]

EOF
}

uninstall() {
    print_warning "Uninstalling PFRP..."
    
    read -p "Stop and disable services? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        systemctl stop pfrpc 2>/dev/null || true
        systemctl stop pfrps 2>/dev/null || true
        systemctl disable pfrpc 2>/dev/null || true
        systemctl disable pfrps 2>/dev/null || true
        print_success "Services stopped and disabled"
    fi
    
    rm -f ${SERVICE_DIR}/pfrpc.service
    rm -f ${SERVICE_DIR}/pfrps.service
    rm -f ${INSTALL_DIR}/pfrpc
    rm -f ${INSTALL_DIR}/pfrps
    rm -f ${INSTALL_DIR}/pfrp
    rm -rf ${CONFIG_DIR}
    rm -rf ${LOG_DIR}
    
    systemctl daemon-reload
    print_success "PFRP uninstalled successfully"
}

show_usage() {
    cat << EOF
Usage: $0 [OPTION]

PFRP Installer - Install PFRP Client/Server

Options:
  --client           Install PFRP Client only
  --server           Install PFRP Server only
  --both             Install both Client and Server
  --uninstall        Uninstall PFRP
  --mirror           Use GitHub mirror for faster download in China
  -h, --help         Show this help message

If no option is provided, interactive mode will be started.

Examples:
  sudo $0 --client          Install client only
  sudo $0 --server          Install server only
  sudo $0 --both            Install both client and server
  sudo $0 --server --mirror Install server with mirror (recommended in China)
  sudo $0 --uninstall       Uninstall PFRP

EOF
}

main_menu() {
    if [ -n "$INSTALL_MODE" ]; then
        case $INSTALL_MODE in
            client)
                install_client
                ;;
            server)
                install_server
                ;;
            both)
                install_client
                echo ""
                print_info "Installing server..."
                install_server
                ;;
            uninstall)
                uninstall
                ;;
        esac
        return
    fi
    
    print_header
    
    echo "Please select an option:"
    echo ""
    echo "  1) Install PFRP Client (frpc)"
    echo "  2) Install PFRP Server (frps)"
    echo "  3) Install Both"
    echo "  4) Uninstall PFRP"
    echo "  5) Exit"
    echo ""
    read -p "Enter your choice [1-5]: " choice
    
    case $choice in
        1)
            install_client
            ;;
        2)
            install_server
            ;;
        3)
            install_client
            echo ""
            print_info "Installing server..."
            install_server
            ;;
        4)
            uninstall
            ;;
        5)
            print_info "Exiting..."
            exit 0
            ;;
        *)
            print_error "Invalid choice"
            exit 1
            ;;
    esac
}

main() {
    check_root
    
    mkdir -p ${LOG_DIR}
    mkdir -p ${CONFIG_DIR}
    
    detect_arch
    
    while [ $# -gt 0 ]; do
        case "$1" in
            --client)
                INSTALL_MODE="client"
                shift
                ;;
            --server)
                INSTALL_MODE="server"
                shift
                ;;
            --both)
                INSTALL_MODE="both"
                shift
                ;;
            --uninstall)
                INSTALL_MODE="uninstall"
                shift
                ;;
            --mirror)
                USE_MIRROR="true"
                print_info "GitHub mirror enabled for faster download in China"
                shift
                ;;
            -h|--help)
                show_usage
                exit 0
                ;;
            *)
                echo "Error: Unknown option '$1'"
                echo ""
                show_usage
                exit 1
                ;;
        esac
    done
    
    main_menu
}

main "$@"
