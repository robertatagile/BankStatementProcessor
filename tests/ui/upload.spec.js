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

test('upload queues a document without a client-side JSON parse error', async ({ page }) => {
  const dialogs = [];
  page.on('dialog', async dialog => {
    dialogs.push(dialog.message());
    await dialog.dismiss();
  });

  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Bank Statement Processor' })).toBeVisible();

  await page.locator('#file-input').setInputFiles({
    name: 'smoke.pdf',
    mimeType: 'application/pdf',
    buffer: Buffer.from(MINIMAL_PDF, 'utf-8')
  });

  await expect(page.locator('#processing-overlay')).not.toHaveClass(/hidden/, {
    timeout: 10_000
  });
  await expect(page.locator('#history-list .history-item').first()).toBeVisible({
    timeout: 10_000
  });
  expect(dialogs).toEqual([]);
});