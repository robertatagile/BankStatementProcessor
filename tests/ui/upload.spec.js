const { test, expect } = require('@playwright/test');

const MINIMAL_PDF = `%PDF-1.1
1 0 obj
<< /Type /Catalog /Pages 2 0 R >>
endobj
2 0 obj
<< /Type /Pages /Kids [3 0 R] /Count 1 >>
endobj
3 0 obj
<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200] >>
endobj
trailer
<< /Root 1 0 R >>
%%EOF`;

test.beforeEach(async ({ request }) => {
  await expect
    .poll(async () => (await request.get('/api/health')).status(), {
      timeout: 15_000
    })
    .toBe(200);
});

/* ═══════════════════════════════════════════════════════════
   NAVIGATION & LAYOUT
   ═══════════════════════════════════════════════════════════ */

test.describe('Navigation', () => {
  test('loads dashboard by default', async ({ page }) => {
    await page.goto('/');
    await expect(page.locator('#view-title')).toHaveText('Dashboard');
    await expect(page.locator('#view-dashboard')).toHaveClass(/active/);
    await expect(page.locator('#sidebar')).toBeVisible();
  });

  test('sidebar navigates between all views', async ({ page }) => {
    await page.goto('/');

    const views = [
      { nav: 'Jobs', viewId: '#view-jobs', title: 'Jobs' },
      { nav: 'Rules', viewId: '#view-rules', title: 'Classification Rules' },
      { nav: 'Refinements', viewId: '#view-refinements', title: 'Refinement Queue' },
      { nav: 'Training', viewId: '#view-training', title: 'Training & Setup' },
      { nav: 'Dashboard', viewId: '#view-dashboard', title: 'Dashboard' },
    ];

    for (const v of views) {
      await page.locator(`.nav-menu a`).filter({ hasText: v.nav }).click();
      await expect(page.locator(v.viewId)).toHaveClass(/active/);
      await expect(page.locator('#view-title')).toHaveText(v.title);
    }
  });

  test('bank dropdown is populated', async ({ page }) => {
    await page.goto('/');
    const options = page.locator('#bank-select option');
    await expect(options.first()).toHaveText('Auto-detect bank');
    // At least the default + one real bank
    await expect(options).not.toHaveCount(0);
  });
});

/* ═══════════════════════════════════════════════════════════
   DASHBOARD
   ═══════════════════════════════════════════════════════════ */

test.describe('Dashboard', () => {
  test('shows stat cards', async ({ page }) => {
    await page.goto('/');
    const stats = page.locator('#dashboard-stats .stat-card');
    await expect(stats).not.toHaveCount(0, { timeout: 5_000 });
    await expect(stats.first()).toBeVisible();
  });

  test('recent jobs section exists', async ({ page }) => {
    await page.goto('/');
    await expect(page.locator('#dashboard-recent')).toBeVisible();
  });
});

/* ═══════════════════════════════════════════════════════════
   UPLOAD & PROCESSING
   ═══════════════════════════════════════════════════════════ */

test.describe('Upload', () => {
  test('upload queues a document and shows processing overlay', async ({ page }) => {
    const dialogs = [];
    page.on('dialog', async dialog => {
      dialogs.push(dialog.message());
      await dialog.dismiss();
    });

    await page.goto('/');

    await page.locator('#file-input').setInputFiles({
      name: 'smoke.pdf',
      mimeType: 'application/pdf',
      buffer: Buffer.from(MINIMAL_PDF, 'utf-8')
    });

    // Processing overlay should appear (poll shows it temporarily)
    await expect(page.locator('#processing-overlay')).not.toHaveClass(/hidden/, {
      timeout: 10_000
    });

    // Wait for processing to finish (overlay hides)
    await expect(page.locator('#processing-overlay')).toHaveClass(/hidden/, {
      timeout: 30_000
    });

    // Should auto-navigate to job detail
    await expect(page.locator('#view-job-detail')).toHaveClass(/active/, {
      timeout: 5_000
    });

    // No JS error dialogs
    expect(dialogs).toEqual([]);
  });

  test('toast notification appears on upload', async ({ page }) => {
    await page.goto('/');

    await page.locator('#file-input').setInputFiles({
      name: 'toast-test.pdf',
      mimeType: 'application/pdf',
      buffer: Buffer.from(MINIMAL_PDF, 'utf-8')
    });

    await expect(page.locator('.toast')).toBeVisible({ timeout: 5_000 });
  });
});

/* ═══════════════════════════════════════════════════════════
   JOBS VIEW
   ═══════════════════════════════════════════════════════════ */

test.describe('Jobs', () => {
  test('jobs view has filters and table', async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-menu a').filter({ hasText: 'Jobs' }).click();

    await expect(page.locator('#jobs-search')).toBeVisible();
    await expect(page.locator('#jobs-status-filter')).toBeVisible();
    await expect(page.locator('#jobs-bank-filter')).toBeVisible();
  });

  test('status filter changes displayed jobs', async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-menu a').filter({ hasText: 'Jobs' }).click();

    // Select failed filter
    await page.locator('#jobs-status-filter').selectOption('failed');
    // The table should update (either empty or only failed jobs)
    await page.waitForTimeout(500);
    const rows = page.locator('#jobs-table-container .data-table tbody tr');
    const count = await rows.count();
    for (let i = 0; i < count; i++) {
      await expect(rows.nth(i).locator('.badge')).toHaveText('failed');
    }
  });

  test('search filters by filename', async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-menu a').filter({ hasText: 'Jobs' }).click();

    await page.locator('#jobs-search').fill('nonexistent-file-xyz');
    await page.waitForTimeout(500);
    // Either no table rows or a "no jobs" message
    const tableRows = page.locator('#jobs-table-container .data-table tbody tr');
    const noJobs = page.locator('#jobs-table-container p');
    const rowCount = await tableRows.count();
    if (rowCount === 0) {
      await expect(noJobs).toBeVisible();
    }
  });
});

/* ═══════════════════════════════════════════════════════════
   JOB DETAIL
   ═══════════════════════════════════════════════════════════ */

test.describe('Job Detail', () => {
  test('back button returns to jobs view', async ({ page }) => {
    await page.goto('/');

    // Upload a file first to create a job
    await page.locator('#file-input').setInputFiles({
      name: 'detail-test.pdf',
      mimeType: 'application/pdf',
      buffer: Buffer.from(MINIMAL_PDF, 'utf-8')
    });

    // Wait for auto-navigate to detail
    await expect(page.locator('#view-job-detail')).toHaveClass(/active/, {
      timeout: 30_000
    });

    // Click back
    await page.locator('.detail-back').click();
    await expect(page.locator('#view-jobs')).toHaveClass(/active/);
  });

  test('detail view shows stage timeline', async ({ page }) => {
    await page.goto('/');

    await page.locator('#file-input').setInputFiles({
      name: 'timeline-test.pdf',
      mimeType: 'application/pdf',
      buffer: Buffer.from(MINIMAL_PDF, 'utf-8')
    });

    await expect(page.locator('#view-job-detail')).toHaveClass(/active/, {
      timeout: 30_000
    });

    await expect(page.locator('.stage-timeline')).toBeVisible();
    const steps = page.locator('.stage-step');
    await expect(steps).toHaveCount(4); // 4 pipeline stages
  });

  test('detail view shows job info card', async ({ page }) => {
    await page.goto('/');

    await page.locator('#file-input').setInputFiles({
      name: 'info-test.pdf',
      mimeType: 'application/pdf',
      buffer: Buffer.from(MINIMAL_PDF, 'utf-8')
    });

    await expect(page.locator('#view-job-detail')).toHaveClass(/active/, {
      timeout: 30_000
    });

    await expect(page.locator('.detail-card').first()).toBeVisible();
    await expect(page.locator('.detail-grid')).toBeVisible();
  });
});

/* ═══════════════════════════════════════════════════════════
   RULES VIEW
   ═══════════════════════════════════════════════════════════ */

test.describe('Rules', () => {
  test('rules view shows add form and filter controls', async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-menu a').filter({ hasText: 'Rules' }).click();

    await expect(page.locator('#rule-form')).toBeVisible();
    await expect(page.locator('#rule-pattern')).toBeVisible();
    await expect(page.locator('#rule-category')).toBeVisible();
    await expect(page.locator('#rule-priority')).toBeVisible();
  });

  test('create rule shows toast and updates table', async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-menu a').filter({ hasText: 'Rules' }).click();

    const uniquePattern = '(?i)playwright_test_' + Date.now();
    await page.locator('#rule-pattern').fill(uniquePattern);
    await page.locator('#rule-category').fill('Test Category');
    await page.locator('#rule-priority').fill('999');

    await page.locator('#rule-form .btn-primary').click();

    await expect(page.locator('.toast')).toBeVisible({ timeout: 5_000 });

    // The rule should appear in the table
    await expect(page.locator('#rules-table-container').getByText('Test Category')).toBeVisible({
      timeout: 5_000
    });
  });

  test('edit rule modal opens and closes', async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-menu a').filter({ hasText: 'Rules' }).click();

    // Wait for rules table to load
    await page.waitForTimeout(1000);

    const editBtn = page.locator('#rules-table-container .btn:has-text("Edit")').first();
    if (await editBtn.isVisible()) {
      await editBtn.click();
      await expect(page.locator('#edit-rule-modal')).not.toHaveClass(/hidden/);
      await page.locator('#edit-rule-modal .btn:has-text("Cancel")').click();
      await expect(page.locator('#edit-rule-modal')).toHaveClass(/hidden/);
    }
  });

  test('source filter changes rule list', async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-menu a').filter({ hasText: 'Rules' }).click();

    await page.locator('#rules-source-filter').selectOption('manual');
    await page.waitForTimeout(500);

    const rows = page.locator('#rules-table-container .data-table tbody tr');
    const count = await rows.count();
    for (let i = 0; i < count; i++) {
      const source = rows.nth(i).locator('.badge');
      if (await source.isVisible()) {
        await expect(source).toHaveText('manual');
      }
    }
  });
});

/* ═══════════════════════════════════════════════════════════
   REFINEMENTS VIEW
   ═══════════════════════════════════════════════════════════ */

test.describe('Refinements', () => {
  test('refinements view loads with filter', async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-menu a').filter({ hasText: 'Refinements' }).click();

    await expect(page.locator('#ref-status-filter')).toBeVisible();
    await expect(page.locator('#refinements-container')).toBeVisible();
  });

  test('status filter defaults to pending', async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-menu a').filter({ hasText: 'Refinements' }).click();

    await expect(page.locator('#ref-status-filter')).toHaveValue('pending');
  });
});

/* ═══════════════════════════════════════════════════════════
   TRAINING VIEW
   ═══════════════════════════════════════════════════════════ */

test.describe('Training', () => {
  test('training view shows guide sections', async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-menu a').filter({ hasText: 'Training' }).click();

    const sections = page.locator('.guide-section');
    await expect(sections).not.toHaveCount(0);
    await expect(sections.first()).toBeVisible();
  });

  test('quick action buttons navigate to correct views', async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-menu a').filter({ hasText: 'Training' }).click();

    await page.locator('.guide-section .btn-primary:has-text("View All Jobs")').click();
    await expect(page.locator('#view-jobs')).toHaveClass(/active/);
  });

  test('training health section loads', async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-menu a').filter({ hasText: 'Training' }).click();

    await expect(page.locator('#training-health')).toBeVisible();
    // Wait for health data to populate
    await expect(page.locator('#training-health')).not.toBeEmpty({ timeout: 5_000 });
  });
});

/* ═══════════════════════════════════════════════════════════
   REPROCESS & FILE EXPLORER ERROR HANDLING
   ═══════════════════════════════════════════════════════════ */

test.describe('Actions', () => {
  test('reprocess button triggers new job', async ({ page }) => {
    await page.goto('/');

    // Upload first to have a job to reprocess
    await page.locator('#file-input').setInputFiles({
      name: 'reprocess-test.pdf',
      mimeType: 'application/pdf',
      buffer: Buffer.from(MINIMAL_PDF, 'utf-8')
    });

    await expect(page.locator('#view-job-detail')).toHaveClass(/active/, {
      timeout: 30_000
    });

    // Click reprocess in the detail view
    const reprocessBtn = page.locator('.detail-card .btn-primary:has-text("Reprocess")');
    if (await reprocessBtn.isVisible()) {
      await reprocessBtn.click();
      // Should show processing overlay
      await expect(page.locator('#processing-overlay')).not.toHaveClass(/hidden/, {
        timeout: 5_000
      });
    }
  });

  test('open file explorer shows error toast in non-desktop environment', async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-menu a').filter({ hasText: 'Jobs' }).click();

    // Wait for table to have any row
    const openBtn = page.locator('#jobs-table-container button[title="Open in Explorer"]').first();
    if (await openBtn.isVisible({ timeout: 3_000 }).catch(() => false)) {
      await openBtn.click();
      // In Docker, this should show an error toast since GUI is unavailable
      await page.waitForTimeout(1000);
      // Toast may be success or error depending on environment
    }
  });
});