(() => {
  const links = Array.from(document.querySelectorAll('a')).filter((link) =>
    link.querySelector('img.doc-screenshot'),
  );
  if (!links.length) return;

  const dialog = document.createElement('dialog');
  dialog.className = 'doc-lightbox';
  dialog.innerHTML = `
    <figure class="doc-lightbox-frame">
      <button class="doc-lightbox-close" type="button" aria-label="Close image viewer">&times;</button>
      <img class="doc-lightbox-image" alt="" />
    </figure>
  `;

  document.body.appendChild(dialog);

  const image = dialog.querySelector('.doc-lightbox-image');
  const closeButton = dialog.querySelector('.doc-lightbox-close');

  links.forEach((link) => {
    link.classList.add('doc-lightbox-trigger');
    link.addEventListener('click', (event) => {
      event.preventDefault();
      const thumbnail = link.querySelector('img');
      image.src = link.href;
      image.alt = thumbnail?.alt || '';
      dialog.showModal();
    });
  });

  closeButton.addEventListener('click', () => dialog.close());

  dialog.addEventListener('click', (event) => {
    const rect = dialog.getBoundingClientRect();
    const clickedOutside =
      event.clientX < rect.left ||
      event.clientX > rect.right ||
      event.clientY < rect.top ||
      event.clientY > rect.bottom;

    if (clickedOutside) {
      dialog.close();
    }
  });

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && dialog.open) {
      dialog.close();
    }
  });
})();
