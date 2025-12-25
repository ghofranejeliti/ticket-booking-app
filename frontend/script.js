const API_BASE_URL = 'http://localhost:5000';

// DOM Elements
const generateBtn = document.getElementById('generateBtn');
const refreshBtn = document.getElementById('refreshBtn');
const loading = document.getElementById('loading');
const toast = document.getElementById('toast');
const tabBtns = document.querySelectorAll('.tab-btn');
const tabPanes = document.querySelectorAll('.tab-pane');

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadAllData();
    setupEventListeners();
});

function setupEventListeners() {
    generateBtn.addEventListener('click', generateTickets);
    refreshBtn.addEventListener('click', loadAllData);
    
    tabBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            const tabId = btn.dataset.tab;
            switchTab(tabId);
        });
    });
}

function switchTab(tabId) {
    tabBtns.forEach(btn => btn.classList.remove('active'));
    tabPanes.forEach(pane => pane.classList.remove('active'));
    
    document.querySelector(`[data-tab="${tabId}"]`).classList.add('active');
    document.getElementById(tabId).classList.add('active');
}

async function generateTickets() {
    try {
        showLoading();
        generateBtn.disabled = true;
        
        const response = await fetch(`${API_BASE_URL}/generate-tickets`, {
            method: 'POST'
        });
        
        if (!response.ok) throw new Error('Failed to generate tickets');
        
        const data = await response.json();
        showToast(data.message, 'success');
        
        // Wait a bit for Kafka to process, then refresh
        setTimeout(() => {
            loadAllData();
        }, 2000);
        
    } catch (error) {
        showToast('Error: ' + error.message, 'error');
    } finally {
        hideLoading();
        generateBtn.disabled = false;
    }
}

async function loadAllData() {
    try {
        showLoading();
        
        await Promise.all([
            loadReports(),
            loadTickets(),
            loadTransactions(),
            loadNotifications()
        ]);
        
        showToast('Data refreshed successfully', 'success');
    } catch (error) {
        showToast('Error loading data: ' + error.message, 'error');
    } finally {
        hideLoading();
    }
}

async function loadReports() {
    const response = await fetch(`${API_BASE_URL}/reports`);
    const data = await response.json();
    
    document.getElementById('totalTickets').textContent = data.total_tickets || 0;
    document.getElementById('totalRevenue').textContent = `$${(data.total_revenue || 0).toLocaleString()}`;
}

async function loadTickets() {
    const response = await fetch(`${API_BASE_URL}/tickets`);
    const data = await response.json();
    
    const tbody = document.getElementById('ticketsTable');
    
    if (data.tickets && data.tickets.length > 0) {
        tbody.innerHTML = data.tickets.map(ticket => `
            <tr>
                <td>${ticket.ticket_id}</td>
                <td>${ticket.passenger_id}</td>
                <td>${ticket.from}</td>
                <td>${ticket.to}</td>
                <td>$${ticket.fare}</td>
                <td>${new Date(ticket.created_at).toLocaleString()}</td>
            </tr>
        `).join('');
    } else {
        tbody.innerHTML = '<tr><td colspan="6" class="empty-state">No tickets yet. Click "Generate Tickets" to start.</td></tr>';
    }
}

async function loadTransactions() {
    const response = await fetch(`${API_BASE_URL}/transactions`);
    const data = await response.json();
    
    const tbody = document.getElementById('transactionsTable');
    document.getElementById('totalTransactions').textContent = data.transactions ? data.transactions.length : 0;
    
    if (data.transactions && data.transactions.length > 0) {
        tbody.innerHTML = data.transactions.map(txn => `
            <tr>
                <td>${txn.id}</td>
                <td>${txn.customer_id}</td>
                <td>${txn.customer_email}</td>
                <td>$${txn.total_cost}</td>
                <td>${new Date(txn.processed_at).toLocaleString()}</td>
            </tr>
        `).join('');
    } else {
        tbody.innerHTML = '<tr><td colspan="5" class="empty-state">No transactions yet.</td></tr>';
    }
}

async function loadNotifications() {
    const response = await fetch(`${API_BASE_URL}/notifications`);
    const data = await response.json();
    
    const tbody = document.getElementById('notificationsTable');
    document.getElementById('totalNotifications').textContent = data.notifications ? data.notifications.length : 0;
    
    if (data.notifications && data.notifications.length > 0) {
        tbody.innerHTML = data.notifications.map(notif => `
            <tr>
                <td>${notif.email}</td>
                <td>${new Date(notif.sent_at).toLocaleString()}</td>
            </tr>
        `).join('');
    } else {
        tbody.innerHTML = '<tr><td colspan="2" class="empty-state">No notifications yet.</td></tr>';
    }
}

function showLoading() {
    loading.classList.remove('hidden');
}

function hideLoading() {
    loading.classList.add('hidden');
}

function showToast(message, type = 'success') {
    toast.textContent = message;
    toast.className = `toast ${type}`;
    
    setTimeout(() => {
        toast.classList.add('hidden');
    }, 3000);
}