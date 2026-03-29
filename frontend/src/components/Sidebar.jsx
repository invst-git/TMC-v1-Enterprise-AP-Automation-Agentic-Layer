import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { LayoutDashboard, Bot, ListChecks, Users, AlertTriangle, CreditCard } from 'lucide-react';
import { useLiveRefresh } from '../lib/useLiveRefresh';
import { fetchAgentReviewQueueCounts } from '../services/api';

const Sidebar = ({ activeItem }) => {
  const navigate = useNavigate();
  const [reviewQueueCounts, setReviewQueueCounts] = useState({
    pending_count: 0,
    urgent_count: 0,
  });

  const loadCounts = async () => {
    try {
      const counts = await fetchAgentReviewQueueCounts();
      setReviewQueueCounts(counts || { pending_count: 0, urgent_count: 0 });
    } catch (_) {
      return;
    }
  };

  useLiveRefresh(loadCounts, []);

  const navItems = [
    { id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard, path: '/' },
    { id: 'agent-operations', label: 'Agent Ops', icon: Bot, path: '/agent-operations' },
    {
      id: 'review-queue',
      label: 'Review Queue',
      icon: ListChecks,
      path: '/review-queue',
      badge: reviewQueueCounts.pending_count || 0,
      urgent: (reviewQueueCounts.urgent_count || 0) > 0,
    },
    { id: 'vendors', label: 'Vendors', icon: Users, path: '/vendors' },
    { id: 'exceptions', label: 'Exceptions', icon: AlertTriangle, path: '/exceptions' },
    { id: 'payments', label: 'Payments', icon: CreditCard, path: '/payments' }
  ];

  const handleNavigate = (path) => {
    navigate(path);
  };

  return (
    <div className="fixed left-0 top-0 h-screen w-[220px] bg-white border-r border-gray-200 flex flex-col hidden lg:flex z-50">
      {/* Header */}
      <div className="px-6 py-6">
        <h1 className="text-base font-semibold text-black whitespace-nowrap">
          The <span className="font-signature italic">Matching</span> Company
        </h1>
      </div>

      {/* Navigation */}
      <nav className="flex-1 px-3 mt-6">
        <div className="space-y-2">
          {navItems.map((item) => {
            const Icon = item.icon;
            const isActive = activeItem === item.id;
            
            return (
              <button
                key={item.id}
                onClick={() => handleNavigate(item.path)}
                className={`
                  w-full flex items-center gap-3 px-4 py-2.5 rounded-full
                  transition-all duration-200 relative
                  ${
                    isActive
                      ? 'bg-[#F2F2F2] text-black'
                      : 'bg-white text-black border border-gray-200 hover:bg-gray-50'
                  }
                `}
              >
                {isActive && (
                  <div className="absolute left-0 top-1/2 -translate-y-1/2 w-1 h-6 bg-black rounded-r-full" />
                )}
                <Icon className="w-4 h-4" strokeWidth={2} />
                <span className="text-[14px] font-medium">{item.label}</span>
                {item.badge > 0 && (
                  <span
                    className={`ml-auto min-w-[22px] h-[22px] px-1.5 rounded-full text-[11px] font-semibold flex items-center justify-center ${
                      item.urgent
                        ? 'bg-red-50 text-red-700 border border-red-200'
                        : 'bg-gray-100 text-black border border-gray-200'
                    }`}
                  >
                    {item.badge > 99 ? '99+' : item.badge}
                  </span>
                )}
              </button>
            );
          })}
        </div>
      </nav>

      {/* Footer */}
      <div className="px-6 py-6">
        <div className="w-10 h-10 rounded-full bg-white border border-gray-300 flex items-center justify-center">
          <span className="text-[13px] font-semibold text-black">Ax</span>
        </div>
      </div>
    </div>
  );
};

export default Sidebar;
